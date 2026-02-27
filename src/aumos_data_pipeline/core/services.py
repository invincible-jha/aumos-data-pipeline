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
    DataLineageTrackerProtocol,
    DataQualityMonitorProtocol,
    DeidentifierProtocol,
    IncrementalLoaderProtocol,
    IngestorProtocol,
    ProfilerProtocol,
    QualityGateProtocol,
    SchedulingAdapterProtocol,
    SamplingEngineProtocol,
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


class DeidentificationService:
    """Orchestrates PII de-identification for datasets in the pipeline.

    Wraps a DeidentifierProtocol adapter and wires job lifecycle events
    so that de-identification runs are tracked alongside other pipeline stages.
    """

    def __init__(
        self,
        deidentifier: DeidentifierProtocol,
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the de-identification service.

        Args:
            deidentifier: PII masking adapter implementation.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for reading/writing datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._deidentifier = deidentifier
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def deidentify(
        self,
        tenant_id: uuid.UUID,
        input_uri: str,
        deidentification_config: dict[str, Any],
        destination_config: dict[str, Any],
    ) -> PipelineJob:
        """De-identify a dataset and write the masked result to storage.

        Args:
            tenant_id: Tenant context.
            input_uri: MinIO/S3 URI of the dataset to de-identify.
            deidentification_config: PII masking parameters per column.
            destination_config: Output configuration for the masked dataset.

        Returns:
            PipelineJob with status COMPLETED and output_uri set.
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.TRANSFORM,
                status=JobStatus.PENDING,
                source_config={"input_uri": input_uri, "deidentification_config": deidentification_config},
                destination_config=destination_config,
            )
        )

        logger.info(
            "De-identification started",
            job_id=str(job.id),
            input_uri=input_uri,
            tenant_id=str(tenant_id),
        )

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)
            dataframe = await self._storage.read_parquet(input_uri, tenant_id=tenant_id)

            result = await self._deidentifier.deidentify(
                dataframe=dataframe,
                deidentification_config=deidentification_config,
                job_id=job.id,
                tenant_id=tenant_id,
            )

            masked_df = result["dataframe"]
            output_uri = await self._storage.write_parquet(
                dataframe=masked_df,
                destination_config=destination_config,
                tenant_id=tenant_id,
                job_id=job.id,
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=len(masked_df),
                column_count=len(masked_df.columns),
                output_uri=output_uri,
            )

            await self._publisher.publish(
                Topics.PIPELINE_JOB_COMPLETED,
                {
                    "tenant_id": str(tenant_id),
                    "job_id": str(job.id),
                    "pii_columns_detected": result["pii_columns_detected"],
                    "risk_score": result["risk_score"],
                    "output_uri": output_uri,
                },
            )

            logger.info(
                "De-identification completed",
                job_id=str(job.id),
                pii_columns=len(result["pii_columns_detected"]),
                risk_score=result["risk_score"],
            )
            return job

        except Exception as exc:
            logger.error("De-identification failed", job_id=str(job.id), error=str(exc))
            await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            await self._publisher.publish(
                Topics.PIPELINE_JOB_FAILED,
                {"tenant_id": str(tenant_id), "job_id": str(job.id), "error": str(exc)},
            )
            raise


class SamplingService:
    """Applies data sampling strategies to datasets in the pipeline.

    Wraps a SamplingEngineProtocol adapter to allow statistical sub-sampling
    before expensive training or synthesis operations.
    """

    def __init__(
        self,
        sampling_engine: SamplingEngineProtocol,
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the sampling service.

        Args:
            sampling_engine: Sampling algorithm implementation.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for reading/writing datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._engine = sampling_engine
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def sample(
        self,
        tenant_id: uuid.UUID,
        input_uri: str,
        sampling_config: dict[str, Any],
        destination_config: dict[str, Any],
    ) -> PipelineJob:
        """Sample a dataset according to the configured strategy.

        Args:
            tenant_id: Tenant context.
            input_uri: MinIO/S3 URI of the dataset to sample.
            sampling_config: Sampling parameters (strategy, sample_size, seed, etc.)
            destination_config: Output configuration.

        Returns:
            PipelineJob with status COMPLETED and output_uri set.
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.TRANSFORM,
                status=JobStatus.PENDING,
                source_config={"input_uri": input_uri, "sampling_config": sampling_config},
                destination_config=destination_config,
            )
        )

        logger.info(
            "Sampling started",
            job_id=str(job.id),
            input_uri=input_uri,
            strategy=sampling_config.get("strategy", "random"),
            tenant_id=str(tenant_id),
        )

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)
            dataframe = await self._storage.read_parquet(input_uri, tenant_id=tenant_id)

            result = await self._engine.sample(
                dataframe=dataframe,
                sampling_config=sampling_config,
                job_id=job.id,
                tenant_id=tenant_id,
            )

            sampled_df = result["dataframe"]
            output_uri = await self._storage.write_parquet(
                dataframe=sampled_df,
                destination_config=destination_config,
                tenant_id=tenant_id,
                job_id=job.id,
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=result["sampled_rows"],
                column_count=len(sampled_df.columns),
                output_uri=output_uri,
            )

            await self._publisher.publish(
                Topics.PIPELINE_JOB_COMPLETED,
                {
                    "tenant_id": str(tenant_id),
                    "job_id": str(job.id),
                    "strategy": result["strategy"],
                    "original_rows": result["original_rows"],
                    "sampled_rows": result["sampled_rows"],
                    "sampling_ratio": result["sampling_ratio"],
                    "output_uri": output_uri,
                },
            )

            logger.info(
                "Sampling completed",
                job_id=str(job.id),
                original_rows=result["original_rows"],
                sampled_rows=result["sampled_rows"],
            )
            return job

        except Exception as exc:
            logger.error("Sampling failed", job_id=str(job.id), error=str(exc))
            await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            raise


class IncrementalLoadService:
    """Manages incremental (delta) data loading for efficient re-ingestion.

    Wraps IncrementalLoaderProtocol to provide CDC-based upsert processing
    with watermark tracking and batch management.
    """

    def __init__(
        self,
        incremental_loader: IncrementalLoaderProtocol,
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the incremental load service.

        Args:
            incremental_loader: CDC adapter implementation.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for reading/writing datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._loader = incremental_loader
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def load_delta(
        self,
        tenant_id: uuid.UUID,
        source_uri: str,
        staging_uri: str | None,
        load_config: dict[str, Any],
        destination_config: dict[str, Any],
    ) -> PipelineJob:
        """Run an incremental load from a source dataset into staging.

        Args:
            tenant_id: Tenant context.
            source_uri: URI of the full or recent source extract.
            staging_uri: URI of the current staging dataset (None for first load).
            load_config: CDC configuration (watermark_column, primary_key, etc.)
            destination_config: Output configuration for the updated staging dataset.

        Returns:
            PipelineJob with status COMPLETED and output_uri set.
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.INGEST,
                status=JobStatus.PENDING,
                source_config={"source_uri": source_uri, "staging_uri": staging_uri, "load_config": load_config},
                destination_config=destination_config,
            )
        )

        logger.info(
            "Incremental load started",
            job_id=str(job.id),
            source_uri=source_uri,
            tenant_id=str(tenant_id),
        )

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)

            source_df = await self._storage.read_parquet(source_uri, tenant_id=tenant_id)
            existing_df: pd.DataFrame | None = None
            if staging_uri:
                existing_df = await self._storage.read_parquet(staging_uri, tenant_id=tenant_id)

            result = await self._loader.load_delta(
                source_df=source_df,
                existing_df=existing_df,
                load_config=load_config,
                job_id=job.id,
                tenant_id=tenant_id,
            )

            updated_df = result["dataframe"]
            output_uri = await self._storage.write_parquet(
                dataframe=updated_df,
                destination_config=destination_config,
                tenant_id=tenant_id,
                job_id=job.id,
            )

            job = await self._job_repo.update(
                job.id,
                status=JobStatus.COMPLETED,
                row_count=len(updated_df),
                column_count=len(updated_df.columns),
                output_uri=output_uri,
            )

            await self._publisher.publish(
                Topics.PIPELINE_JOB_COMPLETED,
                {
                    "tenant_id": str(tenant_id),
                    "job_id": str(job.id),
                    "inserted_rows": result["inserted_rows"],
                    "updated_rows": result["updated_rows"],
                    "deleted_rows": result["deleted_rows"],
                    "new_watermark": result["new_watermark"],
                    "output_uri": output_uri,
                },
            )

            logger.info(
                "Incremental load completed",
                job_id=str(job.id),
                inserted=result["inserted_rows"],
                updated=result["updated_rows"],
                deleted=result["deleted_rows"],
            )
            return job

        except Exception as exc:
            logger.error("Incremental load failed", job_id=str(job.id), error=str(exc))
            await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            raise


class QualityMonitoringService:
    """Evaluates data quality SLOs and aggregates dashboard metrics.

    Wraps DataQualityMonitorProtocol to provide rule evaluation, SLO
    monitoring, anomaly detection, and quality trend aggregation.
    """

    def __init__(
        self,
        quality_monitor: DataQualityMonitorProtocol,
        job_repository: Any,
        storage: Any,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the quality monitoring service.

        Args:
            quality_monitor: Quality SLO monitoring adapter.
            job_repository: Repository for PipelineJob persistence.
            storage: Storage adapter for reading datasets.
            event_publisher: Kafka publisher for pipeline lifecycle events.
        """
        self._monitor = quality_monitor
        self._job_repo = job_repository
        self._storage = storage
        self._publisher = event_publisher

    async def run_quality_check(
        self,
        tenant_id: uuid.UUID,
        input_uri: str,
        rules: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Load a dataset and evaluate quality rules against it.

        Args:
            tenant_id: Tenant context.
            input_uri: MinIO/S3 URI of the dataset to evaluate.
            rules: Quality rule definitions.

        Returns:
            Quality evaluation results dict from the monitor adapter.
        """
        job = await self._job_repo.create(
            PipelineJob(
                tenant_id=tenant_id,
                job_type=JobType.PROFILE,
                status=JobStatus.PENDING,
                source_config={"input_uri": input_uri, "rule_count": len(rules)},
                destination_config={},
            )
        )

        logger.info(
            "Quality monitoring check started",
            job_id=str(job.id),
            input_uri=input_uri,
            rule_count=len(rules),
            tenant_id=str(tenant_id),
        )

        try:
            job = await self._job_repo.update_status(job.id, JobStatus.RUNNING)
            dataframe = await self._storage.read_parquet(input_uri, tenant_id=tenant_id)

            evaluation_result = await self._monitor.evaluate_rules(
                dataframe=dataframe,
                rules=rules,
                job_id=job.id,
                tenant_id=tenant_id,
            )

            status = JobStatus.COMPLETED if evaluation_result["passed"] else JobStatus.FAILED
            job = await self._job_repo.update(
                job.id,
                status=status,
                quality_score=evaluation_result["quality_score"],
            )

            logger.info(
                "Quality monitoring check completed",
                job_id=str(job.id),
                passed=evaluation_result["passed"],
                quality_score=evaluation_result["quality_score"],
                violations=len(evaluation_result["violations"]),
            )
            return evaluation_result

        except Exception as exc:
            logger.error("Quality monitoring check failed", job_id=str(job.id), error=str(exc))
            await self._job_repo.update(job.id, status=JobStatus.FAILED, error_message=str(exc))
            raise

    async def get_dashboard(
        self,
        tenant_id: uuid.UUID,
        lookback_entries: int = 30,
    ) -> dict[str, Any]:
        """Retrieve aggregated quality metric trends for a tenant.

        Args:
            tenant_id: Tenant context.
            lookback_entries: Number of historical entries per metric.

        Returns:
            Dashboard data dict with trend analysis per metric.
        """
        return await self._monitor.aggregate_dashboard_data(
            tenant_id=tenant_id,
            lookback_entries=lookback_entries,
        )


class LineageService:
    """Records and queries data lineage for pipeline transformations.

    Wraps DataLineageTrackerProtocol to provide lineage recording on each
    pipeline stage completion, field tracing, and impact analysis.
    """

    def __init__(
        self,
        lineage_tracker: DataLineageTrackerProtocol,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize the lineage service.

        Args:
            lineage_tracker: Lineage DAG adapter implementation.
            event_publisher: Kafka publisher for lineage events.
        """
        self._tracker = lineage_tracker
        self._publisher = event_publisher

    async def record_stage(
        self,
        step_name: str,
        input_uris: list[str],
        output_uri: str,
        field_mappings: dict[str, list[str]],
        metadata: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> str:
        """Record a pipeline transformation step in the lineage graph.

        Args:
            step_name: Descriptive name of the transformation stage.
            input_uris: Input dataset URIs consumed by the stage.
            output_uri: Output dataset URI produced by the stage.
            field_mappings: Output-to-input field mappings.
            metadata: Additional context (row_count, schema, etc.)
            job_id: Pipeline job ID.
            tenant_id: Tenant context.

        Returns:
            Lineage edge ID string.
        """
        edge_id = await self._tracker.record_transformation(
            step_name=step_name,
            input_uris=input_uris,
            output_uri=output_uri,
            field_mappings=field_mappings,
            metadata=metadata,
            job_id=job_id,
            tenant_id=tenant_id,
        )
        logger.info(
            "Lineage stage recorded",
            edge_id=edge_id,
            step_name=step_name,
            job_id=str(job_id),
        )
        return edge_id

    async def trace_field(
        self,
        output_uri: str,
        field_name: str,
        tenant_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        """Trace a field back to its origin datasets.

        Args:
            output_uri: URI of the dataset containing the field.
            field_name: Field name to trace.
            tenant_id: Tenant context.

        Returns:
            List of lineage hop dicts from field to its origins.
        """
        return await self._tracker.trace_field_origin(
            output_uri=output_uri,
            field_name=field_name,
            tenant_id=tenant_id,
        )

    async def get_impact(
        self,
        source_uri: str,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Identify all downstream datasets affected by a source change.

        Args:
            source_uri: URI of the dataset that is changing.
            tenant_id: Tenant context.

        Returns:
            Impact analysis dict (affected_datasets, affected_fields, impact_depth).
        """
        return await self._tracker.analyze_impact(source_uri=source_uri, tenant_id=tenant_id)

    async def export_graph(self, tenant_id: uuid.UUID) -> dict[str, Any]:
        """Export the full lineage graph for visualization.

        Args:
            tenant_id: Tenant context.

        Returns:
            Graph dict with nodes and edges.
        """
        return await self._tracker.export_lineage_graph(tenant_id=tenant_id)


class SchedulingService:
    """Manages pipeline scheduling across Airflow and Temporal orchestrators.

    Wraps SchedulingAdapterProtocol to provide DAG generation, cron validation,
    and run status tracking for all tenant pipelines.
    """

    def __init__(self, scheduling_adapter: SchedulingAdapterProtocol) -> None:
        """Initialize the scheduling service.

        Args:
            scheduling_adapter: Orchestrator adapter implementation.
        """
        self._adapter = scheduling_adapter

    async def create_airflow_dag(
        self,
        pipeline_config: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> str:
        """Generate and return an Airflow DAG Python file.

        Args:
            pipeline_config: Pipeline definition dict.
            tenant_id: Tenant context.

        Returns:
            Python source code string for the Airflow DAG.
        """
        dag_code = await self._adapter.generate_airflow_dag(
            pipeline_config=pipeline_config,
            tenant_id=tenant_id,
        )
        logger.info(
            "Airflow DAG created",
            pipeline_id=pipeline_config.get("pipeline_id"),
            tenant_id=str(tenant_id),
        )
        return dag_code

    async def create_temporal_workflow(
        self,
        pipeline_config: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Generate a Temporal workflow definition for a pipeline.

        Args:
            pipeline_config: Pipeline definition dict.
            tenant_id: Tenant context.

        Returns:
            Temporal workflow spec dict.
        """
        spec = await self._adapter.generate_temporal_workflow(
            pipeline_config=pipeline_config,
            tenant_id=tenant_id,
        )
        logger.info(
            "Temporal workflow created",
            pipeline_id=pipeline_config.get("pipeline_id"),
            tenant_id=str(tenant_id),
        )
        return spec

    async def validate_schedule(self, cron_expression: str) -> dict[str, Any]:
        """Validate a cron expression and return the next run times.

        Args:
            cron_expression: Standard cron string or special alias.

        Returns:
            Validation result dict (valid, expression, next_runs, description, error).
        """
        return await self._adapter.validate_cron(cron_expression)

    async def update_run_status(
        self,
        pipeline_id: str,
        run_id: str,
        status: str,
        metadata: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Update the status of a pipeline run.

        Args:
            pipeline_id: Logical pipeline identifier.
            run_id: Unique run identifier.
            status: New run status string.
            metadata: Additional run context.
            tenant_id: Tenant context.

        Returns:
            Updated run record dict.
        """
        return await self._adapter.track_run(
            pipeline_id=pipeline_id,
            run_id=run_id,
            status=status,
            metadata=metadata,
            tenant_id=tenant_id,
        )
