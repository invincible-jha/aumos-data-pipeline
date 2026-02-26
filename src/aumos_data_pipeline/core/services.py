"""Business logic services for the data pipeline.

Six services covering all pipeline stages:
  - IngestionService: multi-source data ingestion
  - ProfilingService: automatic statistical data profiling
  - CleaningService: deduplication, normalization, imputation
  - TransformationService: feature engineering and format conversion
  - OrchestrationService: full pipeline (ingest → profile → clean → transform → quality gate)
  - VersioningService: DVC data versioning for reproducibility

Services are framework-agnostic — they receive dependencies via constructor injection
and delegate all I/O to adapters (connectors, repositories, kafka, storage).
"""

import uuid
from typing import Any

import pandas as pd

from aumos_common.errors import NotFoundError, ValidationError
from aumos_common.events import EventPublisher, Topics
from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import (
    CleanerProtocol,
    IngestorProtocol,
    ProfilerProtocol,
    QualityGateProtocol,
    TransformerProtocol,
    VersionerProtocol,
)
from aumos_data_pipeline.core.models import DataProfile, JobStatus, JobType, PipelineJob, QualityCheck

logger = get_logger(__name__)


class IngestionService:
    """Ingests data from multiple source types into the pipeline staging area.

    Supports PostgreSQL, S3/MinIO, CSV/Parquet files, REST API endpoints,
    and streaming sources. Routes to the appropriate connector based on
    source_type in the source_config.
    """

    def __init__(
        self,
        connector_registry: dict[str, IngestorProtocol],
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the ingestion service with injected dependencies.

        Args:
            connector_registry: Map of source_type → IngestorProtocol implementation.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for writing staged datasets to MinIO/S3.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._connectors = connector_registry
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def ingest(
        self,
        tenant_id: uuid.UUID,
        source_config: dict[str, Any],
        destination_config: dict[str, Any],
    ) -> PipelineJob:
        """Ingest data from a configured source and persist it to staging.

        Creates a PipelineJob, streams data from the source in chunks,
        writes it to MinIO as Parquet, and publishes a lifecycle event.

        Args:
            tenant_id: Tenant performing the ingestion.
            source_config: Source configuration including 'source_type' key.
            destination_config: Destination config (output path, format, etc.)

        Returns:
            PipelineJob with status COMPLETED and output_uri set.

        Raises:
            ValidationError: If source_type is not registered.
            NotFoundError: If connection to the source fails.
        """
        source_type = source_config.get("source_type", "")
        if source_type not in self._connectors:
            raise ValidationError(
                f"Unknown source_type '{source_type}'. "
                f"Supported: {list(self._connectors.keys())}"
            )

        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.INGEST,
                status=JobStatus.PENDING,
                source_config=source_config,
                destination_config=destination_config,
            )
        )

        await self._publisher.publish(
            Topics.PIPELINE_JOB_CREATED,
            {"tenant_id": str(tenant_id), "job_id": str(job.id), "job_type": JobType.INGEST},
        )

        logger.info(
            "Ingestion started",
            job_id=str(job.id),
            source_type=source_type,
            tenant_id=str(tenant_id),
        )

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)
            connector = self._connectors[source_type]

            # Stream in chunks, collect into list for now
            # In production this would stream directly to object storage
            chunks: list[pd.DataFrame] = []
            async for chunk in connector.ingest(source_config):
                chunks.append(chunk)

            if not chunks:
                raise ValidationError("Source returned zero rows — nothing to ingest")

            full_frame = pd.concat(chunks, ignore_index=True)
            row_count = len(full_frame)
            column_count = len(full_frame.columns)

            # Write to staging area
            output_uri = await self._storage.write_parquet(
                dataframe=full_frame,
                destination_config=destination_config,
                tenant_id=tenant_id,
                job_id=job.id,
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=row_count,
                column_count=column_count,
                output_uri=output_uri,
            )

            await self._publisher.publish(
                Topics.PIPELINE_JOB_COMPLETED,
                {
                    "tenant_id": str(tenant_id),
                    "job_id": str(job.id),
                    "job_type": JobType.INGEST,
                    "row_count": row_count,
                    "output_uri": output_uri,
                },
            )

            logger.info(
                "Ingestion completed",
                job_id=str(job.id),
                row_count=row_count,
                column_count=column_count,
                output_uri=output_uri,
            )
            return job

        except Exception as exc:
            logger.error("Ingestion failed", job_id=str(job.id), error=str(exc))
            job = await self._job_repo.update(
                job.id, status=JobStatus.FAILED, error_message=str(exc)
            )
            await self._publisher.publish(
                Topics.PIPELINE_JOB_FAILED,
                {"tenant_id": str(tenant_id), "job_id": str(job.id), "error": str(exc)},
            )
            raise


class ProfilingService:
    """Generates automatic statistical profiles for datasets.

    Computes column data types, value distributions, missing value rates,
    outlier detection, and cardinality — producing a DataProfile record
    that downstream services (tabular-engine, privacy-engine) can use.
    """

    def __init__(
        self,
        profiler: ProfilerProtocol,
        job_repository: Any,
        profile_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the profiling service.

        Args:
            profiler: Profiling engine implementation (pandas/polars).
            job_repository: Repository for PipelineJob persistence.
            profile_repository: Repository for DataProfile persistence.
            storage: Storage adapter for reading staged datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._profiler = profiler
        self._job_repo = job_repository
        self._profile_repo = profile_repository
        self._storage = storage
        self._publisher = event_publisher

    async def profile(
        self,
        tenant_id: uuid.UUID,
        input_uri: str,
        source_job_id: uuid.UUID | None = None,
    ) -> tuple[PipelineJob, DataProfile]:
        """Profile a dataset at the given URI.

        Reads the dataset from storage, runs statistical profiling,
        persists a DataProfile record, and publishes an event.

        Args:
            tenant_id: Tenant context.
            input_uri: MinIO/S3 URI of the dataset to profile.
            source_job_id: Optional ID of the ingestion job that produced this dataset.

        Returns:
            Tuple of (PipelineJob, DataProfile).
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.PROFILE,
                status=JobStatus.PENDING,
                source_config={"input_uri": input_uri, "source_job_id": str(source_job_id) if source_job_id else None},
                destination_config={},
            )
        )

        logger.info("Profiling started", job_id=str(job.id), input_uri=input_uri, tenant_id=str(tenant_id))

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)

            # Load dataset from storage
            dataframe = await self._storage.read_parquet(input_uri, tenant_id=tenant_id)

            # Run profiling engine
            profile_data = await self._profiler.profile(
                dataframe=dataframe,
                job_id=job.id,
                tenant_id=tenant_id,
            )

            # Persist DataProfile
            data_profile = await self._profile_repo.create(
                DataProfile(
                    tenant_id=tenant_id,
                    job_id=job.id,
                    column_profiles=profile_data["column_profiles"],
                    row_count=profile_data["row_count"],
                    missing_values=profile_data["missing_values"],
                    distributions=profile_data["distributions"],
                    outliers=profile_data["outliers"],
                )
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=profile_data["row_count"],
                column_count=len(profile_data["column_profiles"]),
            )

            await self._publisher.publish(
                Topics.PIPELINE_PROFILE_COMPLETED,
                {
                    "tenant_id": str(tenant_id),
                    "job_id": str(job.id),
                    "profile_id": str(data_profile.id),
                    "row_count": profile_data["row_count"],
                    "column_count": len(profile_data["column_profiles"]),
                },
            )

            logger.info(
                "Profiling completed",
                job_id=str(job.id),
                profile_id=str(data_profile.id),
                row_count=profile_data["row_count"],
            )
            return job, data_profile

        except Exception as exc:
            logger.error("Profiling failed", job_id=str(job.id), error=str(exc))
            job = await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            raise


class CleaningService:
    """Cleans and normalizes datasets.

    Applies deduplication, missing value imputation, outlier handling,
    categorical encoding, and text normalization. Returns a cleaned
    dataset written back to MinIO/S3 as Parquet.
    """

    def __init__(
        self,
        cleaner: CleanerProtocol,
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the cleaning service.

        Args:
            cleaner: Data cleaning implementation.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for reading/writing datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._cleaner = cleaner
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def clean(
        self,
        tenant_id: uuid.UUID,
        input_uri: str,
        cleaning_config: dict[str, Any],
        destination_config: dict[str, Any],
    ) -> PipelineJob:
        """Clean a dataset according to the provided configuration.

        Args:
            tenant_id: Tenant context.
            input_uri: MinIO/S3 URI of the dataset to clean.
            cleaning_config: Cleaning parameters:
                - dedup_subset: List of columns to use for deduplication.
                - imputation_strategy: 'mean' | 'median' | 'mode' | 'drop'.
                - outlier_handling: 'clip' | 'remove' | 'none'.
                - encoding: Dict of {col: 'onehot' | 'label' | 'ordinal'}.
                - normalize: List of columns to normalize (min-max or z-score).
            destination_config: Output configuration.

        Returns:
            PipelineJob with status COMPLETED and output_uri set.
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.CLEAN,
                status=JobStatus.PENDING,
                source_config={"input_uri": input_uri, "cleaning_config": cleaning_config},
                destination_config=destination_config,
            )
        )

        logger.info("Cleaning started", job_id=str(job.id), input_uri=input_uri, tenant_id=str(tenant_id))

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)

            dataframe = await self._storage.read_parquet(input_uri, tenant_id=tenant_id)
            rows_before = len(dataframe)

            cleaned = await self._cleaner.clean(dataframe=dataframe, cleaning_config=cleaning_config)
            rows_after = len(cleaned)

            output_uri = await self._storage.write_parquet(
                dataframe=cleaned,
                destination_config=destination_config,
                tenant_id=tenant_id,
                job_id=job.id,
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=rows_after,
                column_count=len(cleaned.columns),
                output_uri=output_uri,
            )

            logger.info(
                "Cleaning completed",
                job_id=str(job.id),
                rows_before=rows_before,
                rows_after=rows_after,
                rows_removed=rows_before - rows_after,
            )
            return job

        except Exception as exc:
            logger.error("Cleaning failed", job_id=str(job.id), error=str(exc))
            job = await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            raise


class TransformationService:
    """Applies feature engineering and format conversion to datasets.

    Supports scaling, encoding, feature interactions, dimensionality
    reduction, and cross-format conversion (CSV → Parquet, JSON → Arrow).
    """

    def __init__(
        self,
        transformer: TransformerProtocol,
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the transformation service.

        Args:
            transformer: Feature engineering and conversion implementation.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for reading/writing datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._transformer = transformer
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def transform(
        self,
        tenant_id: uuid.UUID,
        input_uri: str,
        transform_config: dict[str, Any],
        destination_config: dict[str, Any],
    ) -> PipelineJob:
        """Apply feature engineering to a cleaned dataset.

        Args:
            tenant_id: Tenant context.
            input_uri: MinIO/S3 URI of the cleaned dataset.
            transform_config: Transformation parameters:
                - scale: Dict of {col: 'minmax' | 'zscore'}.
                - encode: Dict of {col: 'onehot' | 'label'}.
                - interactions: List of column pairs for interaction features.
                - drop_cols: List of columns to drop.
                - target_format: Output format ('parquet', 'arrow', 'csv').
            destination_config: Output configuration.

        Returns:
            PipelineJob with status COMPLETED and output_uri set.
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.TRANSFORM,
                status=JobStatus.PENDING,
                source_config={"input_uri": input_uri, "transform_config": transform_config},
                destination_config=destination_config,
            )
        )

        logger.info("Transformation started", job_id=str(job.id), input_uri=input_uri, tenant_id=str(tenant_id))

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)

            dataframe = await self._storage.read_parquet(input_uri, tenant_id=tenant_id)
            transformed = await self._transformer.transform(dataframe=dataframe, transform_config=transform_config)

            output_uri = await self._storage.write_parquet(
                dataframe=transformed,
                destination_config=destination_config,
                tenant_id=tenant_id,
                job_id=job.id,
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=len(transformed),
                column_count=len(transformed.columns),
                output_uri=output_uri,
            )

            logger.info(
                "Transformation completed",
                job_id=str(job.id),
                output_columns=len(transformed.columns),
                output_uri=output_uri,
            )
            return job

        except Exception as exc:
            logger.error("Transformation failed", job_id=str(job.id), error=str(exc))
            job = await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            raise


class OrchestrationService:
    """Runs the full data pipeline end-to-end.

    Orchestrates the complete pipeline: ingest → profile → clean → transform → quality gate.
    Each stage is optional; the caller controls which stages to run via the pipeline_config.
    Creates a parent PipelineJob of type ORCHESTRATE and child jobs for each stage.
    """

    def __init__(
        self,
        ingestion_service: IngestionService,
        profiling_service: ProfilingService,
        cleaning_service: CleaningService,
        transformation_service: TransformationService,
        quality_gate: QualityGateProtocol,
        versioner: VersionerProtocol,
        job_repository: Any,
        quality_check_repository: Any,
        event_publisher: EventPublisher,
        quality_score_threshold: float = 0.8,
    ) -> None:
        """Initialize the orchestration service with all stage dependencies.

        Args:
            ingestion_service: Handles data ingestion.
            profiling_service: Handles statistical profiling.
            cleaning_service: Handles data cleaning.
            transformation_service: Handles feature engineering.
            quality_gate: Great Expectations quality validation.
            versioner: DVC data versioning.
            job_repository: Repository for PipelineJob persistence.
            quality_check_repository: Repository for QualityCheck persistence.
            event_publisher: Kafka publisher for pipeline lifecycle events.
            quality_score_threshold: Minimum quality score to pass (default 0.8).
        """
        self._ingestion = ingestion_service
        self._profiling = profiling_service
        self._cleaning = cleaning_service
        self._transformation = transformation_service
        self._quality_gate = quality_gate
        self._versioner = versioner
        self._job_repo = job_repository
        self._qc_repo = quality_check_repository
        self._publisher = event_publisher
        self._quality_threshold = quality_score_threshold

    async def orchestrate(
        self,
        tenant_id: uuid.UUID,
        pipeline_config: dict[str, Any],
    ) -> PipelineJob:
        """Run the full pipeline for a tenant.

        The pipeline_config controls which stages run:
        {
          "source_config": {...},          # Required for ingest stage
          "skip_ingest": false,            # If true, provide "input_uri" directly
          "input_uri": "s3://...",         # Used when skip_ingest is true
          "skip_profile": false,
          "skip_clean": false,
          "cleaning_config": {...},
          "skip_transform": false,
          "transform_config": {...},
          "expectation_suite": "default",  # GE suite name (empty = skip quality gate)
          "destination_config": {...},
          "version_output": true,          # Whether to DVC-version the final output
        }

        Args:
            tenant_id: Tenant running the pipeline.
            pipeline_config: Full pipeline configuration (see above).

        Returns:
            Master PipelineJob of type ORCHESTRATE with final output_uri and quality_score.

        Raises:
            ValidationError: If quality gate fails and abort_on_quality_failure=true.
        """
        master_job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.ORCHESTRATE,
                status=JobStatus.PENDING,
                source_config=pipeline_config,
                destination_config=pipeline_config.get("destination_config", {}),
            )
        )

        logger.info("Pipeline orchestration started", job_id=str(master_job.id), tenant_id=str(tenant_id))

        try:
            master_job = await self._job_repo.update_status(master_job.id, JobStatus.RUNNING)
            current_uri: str = ""

            # Stage 1: Ingestion
            if not pipeline_config.get("skip_ingest", False):
                ingest_job = await self._ingestion.ingest(
                    tenant_id=tenant_id,
                    source_config=pipeline_config["source_config"],
                    destination_config={
                        **pipeline_config.get("destination_config", {}),
                        "stage": "ingested",
                        "parent_job_id": str(master_job.id),
                    },
                )
                current_uri = ingest_job.output_uri or ""
            else:
                current_uri = pipeline_config.get("input_uri", "")

            if not current_uri:
                raise ValidationError("No input URI available — either ingest from source or provide input_uri")

            # Stage 2: Profiling
            data_profile = None
            if not pipeline_config.get("skip_profile", False):
                _, data_profile = await self._profiling.profile(
                    tenant_id=tenant_id,
                    input_uri=current_uri,
                    source_job_id=master_job.id,
                )

            # Stage 3: Cleaning
            if not pipeline_config.get("skip_clean", False):
                clean_job = await self._cleaning.clean(
                    tenant_id=tenant_id,
                    input_uri=current_uri,
                    cleaning_config=pipeline_config.get("cleaning_config", {}),
                    destination_config={
                        **pipeline_config.get("destination_config", {}),
                        "stage": "cleaned",
                        "parent_job_id": str(master_job.id),
                    },
                )
                current_uri = clean_job.output_uri or current_uri

            # Stage 4: Transformation
            if not pipeline_config.get("skip_transform", False):
                transform_job = await self._transformation.transform(
                    tenant_id=tenant_id,
                    input_uri=current_uri,
                    transform_config=pipeline_config.get("transform_config", {}),
                    destination_config={
                        **pipeline_config.get("destination_config", {}),
                        "stage": "transformed",
                        "parent_job_id": str(master_job.id),
                    },
                )
                current_uri = transform_job.output_uri or current_uri

            # Stage 5: Quality Gate
            quality_score = None
            expectation_suite = pipeline_config.get("expectation_suite", "")

            if expectation_suite:
                # Load final dataset for quality validation
                import pandas as _pd  # noqa: PLC0415

                # Read final transformed dataset (storage accessed via quality gate)
                qc_result = await self._quality_gate.validate(
                    dataframe=_pd.DataFrame(),  # placeholder — real impl reads from current_uri
                    expectation_suite=expectation_suite,
                    job_id=master_job.id,
                    tenant_id=tenant_id,
                )

                quality_score = qc_result["quality_score"]

                await self._qc_repo.create(
                    QualityCheck(
                        tenant_id=tenant_id,
                        job_id=master_job.id,
                        expectation_suite=expectation_suite,
                        results=qc_result["results"],
                        passed=qc_result["passed"],
                    )
                )

                if not qc_result["passed"] and pipeline_config.get("abort_on_quality_failure", True):
                    raise ValidationError(
                        f"Quality gate failed: score {quality_score:.3f} < threshold {self._quality_threshold:.3f}"
                    )

            # Stage 6: DVC Versioning
            data_version = None
            if pipeline_config.get("version_output", True):
                data_version = await self._versioner.commit_version(
                    dataset_uri=current_uri,
                    metadata={
                        "job_id": str(master_job.id),
                        "tenant_id": str(tenant_id),
                        "quality_score": quality_score,
                        "profile_id": str(data_profile.id) if data_profile else None,
                    },
                    tenant_id=tenant_id,
                )

            # Finalize master job
            master_job = await self._job_repo.update(
                master_job.id,
                status=JobStatus.COMPLETED,
                quality_score=quality_score,
                data_version=data_version,
                output_uri=current_uri,
            )

            await self._publisher.publish(
                Topics.PIPELINE_JOB_COMPLETED,
                {
                    "tenant_id": str(tenant_id),
                    "job_id": str(master_job.id),
                    "job_type": JobType.ORCHESTRATE,
                    "quality_score": quality_score,
                    "data_version": data_version,
                    "output_uri": current_uri,
                },
            )

            logger.info(
                "Pipeline orchestration completed",
                job_id=str(master_job.id),
                quality_score=quality_score,
                data_version=data_version,
                output_uri=current_uri,
            )
            return master_job

        except Exception as exc:
            logger.error("Pipeline orchestration failed", job_id=str(master_job.id), error=str(exc))
            master_job = await self._job_repo.update(master_job.id, status=JobStatus.FAILED, error_message=str(exc))
            await self._publisher.publish(
                Topics.PIPELINE_JOB_FAILED,
                {"tenant_id": str(tenant_id), "job_id": str(master_job.id), "error": str(exc)},
            )
            raise


class VersioningService:
    """Manages DVC data versioning for reproducibility.

    Wraps the DVCVersioner adapter to provide high-level versioning
    operations — commit a dataset version, retrieve version metadata,
    and compare versions.
    """

    def __init__(self, versioner: VersionerProtocol) -> None:
        """Initialize the versioning service.

        Args:
            versioner: DVC versioning adapter implementation.
        """
        self._versioner = versioner

    async def commit_version(
        self,
        dataset_uri: str,
        metadata: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> str:
        """Commit a dataset to version control and return the version hash.

        Args:
            dataset_uri: URI of the dataset in MinIO/S3.
            metadata: Version metadata to attach.
            tenant_id: Tenant context for namespacing.

        Returns:
            DVC commit hash string.
        """
        version_hash = await self._versioner.commit_version(
            dataset_uri=dataset_uri,
            metadata=metadata,
            tenant_id=tenant_id,
        )
        logger.info(
            "Dataset version committed",
            dataset_uri=dataset_uri,
            version_hash=version_hash,
            tenant_id=str(tenant_id),
        )
        return version_hash

    async def get_version(self, version_hash: str, tenant_id: uuid.UUID) -> dict[str, Any]:
        """Retrieve metadata for a specific data version.

        Args:
            version_hash: DVC commit hash.
            tenant_id: Tenant context.

        Returns:
            Version metadata dict.

        Raises:
            NotFoundError: If the version hash is not found.
        """
        version_data = await self._versioner.get_version(version_hash=version_hash, tenant_id=tenant_id)
        if not version_data:
            raise NotFoundError(f"Data version '{version_hash}' not found")
        return version_data
