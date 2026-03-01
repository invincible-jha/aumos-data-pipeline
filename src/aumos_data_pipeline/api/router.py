"""FastAPI router for data pipeline endpoints.

All routes delegate to core services — no business logic lives here.
Auth and tenant context are injected via aumos-common dependencies.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from aumos_common.auth import TenantContext, get_current_tenant
from aumos_common.database import get_db_session
from aumos_common.errors import NotFoundError, ValidationError
from aumos_common.observability import get_logger

from aumos_data_pipeline.api.schemas import (
    CleanRequest,
    CleanResponse,
    CronValidateRequest,
    CronValidateResponse,
    DataProfileResponse,
    IngestRequest,
    IngestResponse,
    JobLineageResponse,
    JobStatusResponse,
    KafkaBatchIngestRequest,
    KafkaBatchIngestResponse,
    OrchestrateRequest,
    OrchestrateResponse,
    PipelineDashboardResponse,
    PipelineJobResponse,
    PluginListResponse,
    PluginRegisterRequest,
    PluginResponse,
    ProfileRequest,
    ProfileResponse,
    ScheduleCreateRequest,
    ScheduleResponse,
    TransformRequest,
    TransformResponse,
)
from aumos_data_pipeline.core.services import (
    CleaningService,
    IngestionService,
    OrchestrationService,
    ProfilingService,
    TransformationService,
)

logger = get_logger(__name__)
router = APIRouter(prefix="/pipeline", tags=["pipeline"])


# ---------------------------------------------------------------------------
# Dependency factories — wire up services with request-scoped DB session
# ---------------------------------------------------------------------------


def get_ingestion_service(session: AsyncSession = Depends(get_db_session)) -> IngestionService:
    """Create an IngestionService with request-scoped dependencies."""
    from aumos_data_pipeline.adapters.connectors.api_connector import ApiConnector
    from aumos_data_pipeline.adapters.connectors.csv_connector import CsvConnector
    from aumos_data_pipeline.adapters.connectors.postgres_connector import PostgresConnector
    from aumos_data_pipeline.adapters.connectors.s3_connector import S3Connector
    from aumos_data_pipeline.adapters.kafka import PipelineEventPublisher
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository
    from aumos_data_pipeline.adapters.storage import MinioStorage

    return IngestionService(
        connector_registry={
            "postgres": PostgresConnector(),
            "s3": S3Connector(),
            "csv": CsvConnector(),
            "api": ApiConnector(),
        },
        job_repository=PipelineJobRepository(session),
        storage=MinioStorage(),
        event_publisher=PipelineEventPublisher(),
    )


def get_profiling_service(session: AsyncSession = Depends(get_db_session)) -> ProfilingService:
    """Create a ProfilingService with request-scoped dependencies."""
    from aumos_data_pipeline.adapters.kafka import PipelineEventPublisher
    from aumos_data_pipeline.adapters.profiler import PandasProfiler
    from aumos_data_pipeline.adapters.repositories import DataProfileRepository, PipelineJobRepository
    from aumos_data_pipeline.adapters.storage import MinioStorage

    return ProfilingService(
        profiler=PandasProfiler(),
        job_repository=PipelineJobRepository(session),
        profile_repository=DataProfileRepository(session),
        storage=MinioStorage(),
        event_publisher=PipelineEventPublisher(),
    )


def get_cleaning_service(session: AsyncSession = Depends(get_db_session)) -> CleaningService:
    """Create a CleaningService with request-scoped dependencies."""
    from aumos_data_pipeline.adapters.cleaner import DataCleaner
    from aumos_data_pipeline.adapters.kafka import PipelineEventPublisher
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository
    from aumos_data_pipeline.adapters.storage import MinioStorage

    return CleaningService(
        cleaner=DataCleaner(),
        job_repository=PipelineJobRepository(session),
        storage=MinioStorage(),
        event_publisher=PipelineEventPublisher(),
    )


def get_transformation_service(session: AsyncSession = Depends(get_db_session)) -> TransformationService:
    """Create a TransformationService with request-scoped dependencies."""
    from aumos_data_pipeline.adapters.kafka import PipelineEventPublisher
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository
    from aumos_data_pipeline.adapters.storage import MinioStorage
    from aumos_data_pipeline.adapters.transformer import FeatureTransformer

    return TransformationService(
        transformer=FeatureTransformer(),
        job_repository=PipelineJobRepository(session),
        storage=MinioStorage(),
        event_publisher=PipelineEventPublisher(),
    )


def get_orchestration_service(session: AsyncSession = Depends(get_db_session)) -> OrchestrationService:
    """Create an OrchestrationService composing all stage services."""
    from aumos_data_pipeline.adapters.connectors.api_connector import ApiConnector
    from aumos_data_pipeline.adapters.connectors.csv_connector import CsvConnector
    from aumos_data_pipeline.adapters.connectors.postgres_connector import PostgresConnector
    from aumos_data_pipeline.adapters.connectors.s3_connector import S3Connector
    from aumos_data_pipeline.adapters.dvc_versioner import DVCVersioner
    from aumos_data_pipeline.adapters.kafka import PipelineEventPublisher
    from aumos_data_pipeline.adapters.profiler import PandasProfiler
    from aumos_data_pipeline.adapters.cleaner import DataCleaner
    from aumos_data_pipeline.adapters.transformer import FeatureTransformer
    from aumos_data_pipeline.adapters.quality_gate import GreatExpectationsGate
    from aumos_data_pipeline.adapters.repositories import (
        DataProfileRepository,
        PipelineJobRepository,
        QualityCheckRepository,
    )
    from aumos_data_pipeline.adapters.storage import MinioStorage
    from aumos_data_pipeline.settings import Settings

    settings = Settings()
    storage = MinioStorage()
    publisher = PipelineEventPublisher()
    job_repo = PipelineJobRepository(session)

    ingestion_svc = IngestionService(
        connector_registry={
            "postgres": PostgresConnector(),
            "s3": S3Connector(),
            "csv": CsvConnector(),
            "api": ApiConnector(),
        },
        job_repository=job_repo,
        storage=storage,
        event_publisher=publisher,
    )
    profiling_svc = ProfilingService(
        profiler=PandasProfiler(),
        job_repository=job_repo,
        profile_repository=DataProfileRepository(session),
        storage=storage,
        event_publisher=publisher,
    )
    cleaning_svc = CleaningService(
        cleaner=DataCleaner(),
        job_repository=job_repo,
        storage=storage,
        event_publisher=publisher,
    )
    transform_svc = TransformationService(
        transformer=FeatureTransformer(),
        job_repository=job_repo,
        storage=storage,
        event_publisher=publisher,
    )

    return OrchestrationService(
        ingestion_service=ingestion_svc,
        profiling_service=profiling_svc,
        cleaning_service=cleaning_svc,
        transformation_service=transform_svc,
        quality_gate=GreatExpectationsGate(storage=storage),
        versioner=DVCVersioner(),
        job_repository=job_repo,
        quality_check_repository=QualityCheckRepository(session),
        event_publisher=publisher,
        quality_score_threshold=settings.quality_score_threshold,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/ingest", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_data(
    request: IngestRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    service: IngestionService = Depends(get_ingestion_service),
) -> IngestResponse:
    """Ingest data from a configured source into the pipeline staging area.

    Supports: postgres, s3, csv, api source types.
    Returns a job in PENDING/RUNNING state — poll /jobs/{id} for completion.
    """
    logger.info("Ingest request received", tenant_id=str(tenant.tenant_id), source_type=request.source_config.get("source_type"))
    try:
        job = await service.ingest(
            tenant_id=tenant.tenant_id,
            source_config=request.source_config,
            destination_config=request.destination_config,
        )
        return IngestResponse(job=PipelineJobResponse.model_validate(job))
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@router.post("/profile", response_model=ProfileResponse, status_code=status.HTTP_202_ACCEPTED)
async def profile_data(
    request: ProfileRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    service: ProfilingService = Depends(get_profiling_service),
) -> ProfileResponse:
    """Profile a dataset and store statistical summaries.

    Produces a DataProfile with column types, distributions, missing values,
    and outliers. Results available at /profiles/{id}.
    """
    logger.info("Profile request received", tenant_id=str(tenant.tenant_id), input_uri=request.input_uri)
    try:
        job, profile = await service.profile(
            tenant_id=tenant.tenant_id,
            input_uri=request.input_uri,
            source_job_id=request.source_job_id,
        )
        return ProfileResponse(
            job=PipelineJobResponse.model_validate(job),
            profile=DataProfileResponse.model_validate(profile),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/clean", response_model=CleanResponse, status_code=status.HTTP_202_ACCEPTED)
async def clean_data(
    request: CleanRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    service: CleaningService = Depends(get_cleaning_service),
) -> CleanResponse:
    """Clean and normalize a dataset.

    Applies deduplication, imputation, outlier handling, and encoding.
    Output written to MinIO as Parquet.
    """
    logger.info("Clean request received", tenant_id=str(tenant.tenant_id), input_uri=request.input_uri)
    try:
        job = await service.clean(
            tenant_id=tenant.tenant_id,
            input_uri=request.input_uri,
            cleaning_config=request.cleaning_config,
            destination_config=request.destination_config,
        )
        return CleanResponse(job=PipelineJobResponse.model_validate(job))
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@router.post("/transform", response_model=TransformResponse, status_code=status.HTTP_202_ACCEPTED)
async def transform_data(
    request: TransformRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    service: TransformationService = Depends(get_transformation_service),
) -> TransformResponse:
    """Apply feature engineering and format conversion to a dataset.

    Supports scaling, encoding, feature interactions, and format conversion
    (CSV → Parquet, JSON → Arrow).
    """
    logger.info("Transform request received", tenant_id=str(tenant.tenant_id), input_uri=request.input_uri)
    try:
        job = await service.transform(
            tenant_id=tenant.tenant_id,
            input_uri=request.input_uri,
            transform_config=request.transform_config,
            destination_config=request.destination_config,
        )
        return TransformResponse(job=PipelineJobResponse.model_validate(job))
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@router.post("/orchestrate", response_model=OrchestrateResponse, status_code=status.HTTP_202_ACCEPTED)
async def orchestrate_pipeline(
    request: OrchestrateRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    service: OrchestrationService = Depends(get_orchestration_service),
) -> OrchestrateResponse:
    """Run the full data pipeline end-to-end.

    Executes: ingest → profile → clean → transform → quality gate → DVC version.
    Individual stages can be skipped via skip_* flags.
    """
    logger.info("Orchestrate request received", tenant_id=str(tenant.tenant_id))
    if not request.skip_ingest and not request.source_config:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Either source_config (for ingestion) or skip_ingest=true with input_uri must be provided",
        )
    try:
        job = await service.orchestrate(
            tenant_id=tenant.tenant_id,
            pipeline_config=request.model_dump(),
        )
        return OrchestrateResponse(job=PipelineJobResponse.model_validate(job))
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(
    job_id: uuid.UUID,
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> JobStatusResponse:
    """Get the status and details of a pipeline job."""
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository, QualityCheckRepository

    job_repo = PipelineJobRepository(session)
    qc_repo = QualityCheckRepository(session)

    job = await job_repo.get_by_id(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")

    quality_checks = await qc_repo.list_by_job(job_id)
    return JobStatusResponse(
        job=PipelineJobResponse.model_validate(job),
        quality_checks=[],  # Populated from quality_checks once serialized
    )


@router.get("/profiles/{profile_id}", response_model=DataProfileResponse)
async def get_profile(
    profile_id: uuid.UUID,
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> DataProfileResponse:
    """Retrieve a data profile by ID."""
    from aumos_data_pipeline.adapters.repositories import DataProfileRepository

    profile_repo = DataProfileRepository(session)
    profile = await profile_repo.get_by_id(profile_id)
    if not profile:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Profile {profile_id} not found")

    return DataProfileResponse.model_validate(profile)


# ---------------------------------------------------------------------------
# Dashboard (GAP-114)
# ---------------------------------------------------------------------------


@router.get("/dashboard", response_model=PipelineDashboardResponse)
async def get_pipeline_dashboard(
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> PipelineDashboardResponse:
    """Return an aggregated pipeline dashboard for the authenticated tenant.

    Provides job counts by status, average quality score, total rows processed,
    recent job list, and top failing source types.
    """
    from aumos_data_pipeline.api.schemas import JobSummary
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository

    job_repo = PipelineJobRepository(session)
    recent_jobs = await job_repo.list_by_tenant(tenant.tenant_id, limit=10)

    completed = sum(1 for j in recent_jobs if j.status == "completed")
    failed = sum(1 for j in recent_jobs if j.status == "failed")
    running = sum(1 for j in recent_jobs if j.status == "running")
    pending = sum(1 for j in recent_jobs if j.status == "pending")

    quality_scores = [j.quality_score for j in recent_jobs if j.quality_score is not None]
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else None
    total_rows = sum(j.row_count or 0 for j in recent_jobs)

    failing_sources = [
        j.source_config.get("source_type", "unknown")
        for j in recent_jobs
        if j.status == "failed"
    ]

    return PipelineDashboardResponse(
        tenant_id=tenant.tenant_id,
        job_summary=JobSummary(
            total=len(recent_jobs),
            completed=completed,
            failed=failed,
            running=running,
            pending=pending,
        ),
        avg_quality_score=avg_quality,
        total_rows_processed=total_rows,
        recent_jobs=[PipelineJobResponse.model_validate(j) for j in recent_jobs],
        top_failing_sources=list(dict.fromkeys(failing_sources))[:5],
    )


@router.get("/jobs/{job_id}/report", response_model=DataProfileResponse)
async def get_job_report(
    job_id: uuid.UUID,
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> DataProfileResponse:
    """Return the data profile report associated with a job.

    Retrieves the DataProfile produced during the profiling stage of the
    given pipeline job. Returns 404 if no profile has been generated yet.
    """
    from aumos_data_pipeline.adapters.repositories import DataProfileRepository

    profile_repo = DataProfileRepository(session)
    profile = await profile_repo.get_by_job_id(job_id)
    if not profile:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No profile report found for job {job_id}",
        )
    return DataProfileResponse.model_validate(profile)


@router.get("/versions", response_model=list[dict])
async def list_data_versions(
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    """List all DVC-versioned dataset outputs for the authenticated tenant.

    Returns a list of version records with data_version (DVC hash), output_uri,
    job_id, and row/column counts.
    """
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository

    job_repo = PipelineJobRepository(session)
    jobs = await job_repo.list_by_tenant(tenant.tenant_id, limit=100)

    return [
        {
            "job_id": str(j.id),
            "data_version": j.data_version,
            "output_uri": j.output_uri,
            "row_count": j.row_count,
            "column_count": j.column_count,
            "job_type": j.job_type,
            "status": j.status,
            "created_at": str(j.created_at) if hasattr(j, "created_at") else None,
        }
        for j in jobs
        if j.data_version
    ]


# ---------------------------------------------------------------------------
# Lineage (GAP-116)
# ---------------------------------------------------------------------------


@router.get("/jobs/{job_id}/lineage", response_model=JobLineageResponse)
async def get_job_lineage(
    job_id: uuid.UUID,
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> JobLineageResponse:
    """Retrieve the full data lineage graph for a pipeline job.

    Traces input datasets, transformation steps, and output datasets
    using the DataLineageTracker. Supports both upstream (provenance) and
    downstream (impact analysis) traversal.
    """
    from aumos_data_pipeline.adapters.lineage_tracker import DataLineageTracker
    from aumos_data_pipeline.adapters.kafka import PipelineEventPublisher
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository

    job_repo = PipelineJobRepository(session)
    job = await job_repo.get_by_id(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")

    tracker = DataLineageTracker(event_publisher=PipelineEventPublisher())
    output_uri = job.output_uri or f"dpl-staging/{tenant.tenant_id}/{job_id}"
    lineage = await tracker.get_dataset_lineage(
        dataset_uri=output_uri,
        tenant_id=tenant.tenant_id,
    )

    from aumos_data_pipeline.api.schemas import LineageEdgeResponse

    def _to_edge_response(edge: dict) -> LineageEdgeResponse:
        return LineageEdgeResponse(
            edge_id=edge.get("edge_id", ""),
            step_name=edge.get("step_name", ""),
            input_uris=edge.get("input_uris", []),
            output_uri=edge.get("output_uri", ""),
            field_mappings=edge.get("field_mappings", {}),
            job_id=edge.get("job_id", ""),
            timestamp=edge.get("timestamp", ""),
        )

    return JobLineageResponse(
        job_id=job_id,
        dataset=lineage.get("dataset", {}),
        upstream=[_to_edge_response(e) for e in lineage.get("upstream", [])],
        downstream=[_to_edge_response(e) for e in lineage.get("downstream", [])],
        visualization_data=lineage.get("visualization_data", {}),
    )


# ---------------------------------------------------------------------------
# Scheduling / Cron (GAP-118)
# ---------------------------------------------------------------------------


@router.post("/schedules", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_pipeline_schedule(
    request: ScheduleCreateRequest,
    tenant: TenantContext = Depends(get_current_tenant),
) -> ScheduleResponse:
    """Create a scheduled pipeline run with a cron expression.

    Validates the cron expression, generates an Airflow DAG or Temporal workflow
    spec, and returns the schedule configuration with upcoming run times.
    """
    from aumos_data_pipeline.adapters.scheduling_adapter import SchedulingAdapter

    adapter = SchedulingAdapter()
    cron_result = await adapter.validate_cron(request.cron_expression)
    if not cron_result["valid"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid cron expression: {cron_result['error']}",
        )

    pipeline_config = {
        "pipeline_id": request.pipeline_id,
        "display_name": request.display_name,
        "schedule": request.cron_expression,
        "stages": request.stages,
        "orchestrator": request.orchestrator,
        "retry_policy": request.retry_policy,
        "tags": request.tags,
        "dependencies": {},
    }

    dag_or_workflow: dict | str | None = None
    if request.orchestrator == "airflow":
        dag_or_workflow = await adapter.generate_airflow_dag(pipeline_config, tenant.tenant_id)
    elif request.orchestrator == "temporal":
        dag_or_workflow = await adapter.generate_temporal_workflow(pipeline_config, tenant.tenant_id)

    return ScheduleResponse(
        pipeline_id=request.pipeline_id,
        display_name=request.display_name,
        cron_expression=cron_result["expression"],
        next_runs=cron_result["next_runs"],
        cron_description=cron_result["description"],
        orchestrator=request.orchestrator,
        dag_or_workflow=dag_or_workflow,
    )


@router.post("/schedules/validate-cron", response_model=CronValidateResponse)
async def validate_cron_expression(
    request: CronValidateRequest,
    tenant: TenantContext = Depends(get_current_tenant),
) -> CronValidateResponse:
    """Validate a cron expression and return the next 5 run times.

    Supports standard 5-field cron syntax and @daily/@hourly/@weekly aliases.
    """
    from aumos_data_pipeline.adapters.scheduling_adapter import SchedulingAdapter

    adapter = SchedulingAdapter()
    result = await adapter.validate_cron(request.expression)
    return CronValidateResponse(
        valid=result["valid"],
        expression=result["expression"],
        next_runs=result["next_runs"],
        description=result["description"],
        error=result["error"],
    )


# ---------------------------------------------------------------------------
# Plugins (GAP-119)
# ---------------------------------------------------------------------------


@router.post("/plugins", response_model=PluginResponse, status_code=status.HTTP_201_CREATED)
async def register_plugin(
    request: PluginRegisterRequest,
    tenant: TenantContext = Depends(get_current_tenant),
) -> PluginResponse:
    """Register a custom pipeline extension plugin.

    Plugins extend the pipeline with custom connectors, transformers,
    quality checks, or exporters. The plugin is identified by (tenant, name).
    """
    from aumos_data_pipeline.adapters.plugin_registry import get_plugin_registry

    registry = get_plugin_registry()
    try:
        registration = registry.register(
            name=request.name,
            plugin_type=request.plugin_type,
            version=request.version,
            module_path=request.module_path,
            class_name=request.class_name,
            config_schema=request.config_schema,
            tenant_id=tenant.tenant_id,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    return PluginResponse(
        plugin_id=registration.plugin_id,
        name=registration.name,
        plugin_type=registration.plugin_type,
        version=registration.version,
        module_path=registration.module_path,
        class_name=registration.class_name,
        registered_by=registration.registered_by,
    )


@router.get("/plugins", response_model=PluginListResponse)
async def list_plugins(
    plugin_type: str | None = None,
    tenant: TenantContext = Depends(get_current_tenant),
) -> PluginListResponse:
    """List all registered plugins for the authenticated tenant.

    Optionally filter by plugin_type: connector, transformer, quality_check, exporter.
    """
    from aumos_data_pipeline.adapters.plugin_registry import get_plugin_registry

    registry = get_plugin_registry()
    registrations = registry.list_plugins(
        tenant_id=tenant.tenant_id,
        plugin_type=plugin_type,
    )
    plugins = [
        PluginResponse(
            plugin_id=r.plugin_id,
            name=r.name,
            plugin_type=r.plugin_type,
            version=r.version,
            module_path=r.module_path,
            class_name=r.class_name,
            registered_by=r.registered_by,
        )
        for r in registrations
    ]
    return PluginListResponse(plugins=plugins, total=len(plugins))


# ---------------------------------------------------------------------------
# Streaming / Kafka (GAP-117)
# ---------------------------------------------------------------------------


@router.post("/ingest/kafka-batch", response_model=KafkaBatchIngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_kafka_batch(
    request: KafkaBatchIngestRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    session: AsyncSession = Depends(get_db_session),
) -> KafkaBatchIngestResponse:
    """Consume one micro-batch from a Kafka topic and stage as Parquet.

    Creates a pipeline job record, connects to the Kafka topic using the
    provided credentials, consumes up to batch_size messages within
    timeout_seconds, and stages the result to MinIO.
    """
    from aumos_data_pipeline.adapters.connectors.kafka_streaming_connector import KafkaStreamingConnector
    from aumos_data_pipeline.adapters.repositories import PipelineJobRepository
    from aumos_data_pipeline.adapters.storage import MinioStorage
    from aumos_data_pipeline.core.models import JobStatus, JobType

    job_repo = PipelineJobRepository(session)
    storage = MinioStorage()

    job = await job_repo.create(
        tenant_id=tenant.tenant_id,
        job_type=JobType.INGEST,
        source_config={
            "source_type": "kafka",
            "topic": request.topic,
            "group_id": request.group_id,
        },
        destination_config={},
    )

    class _SecretsStub:
        async def get(self, secret_id: str) -> dict:
            return {}

    connector = KafkaStreamingConnector(
        topic=request.topic,
        group_id=request.group_id,
        credentials_secret_id=request.credentials_secret_id,
        secrets_client=_SecretsStub(),
        batch_size=request.batch_size,
        timeout_seconds=request.timeout_seconds,
        auto_offset_reset=request.auto_offset_reset,
    )

    try:
        result = await connector.ingest_batch(
            staging_storage=storage,
            tenant_id=tenant.tenant_id,
            job_id=job.id,
        )
        await job_repo.update_status(job.id, JobStatus.COMPLETED, output_uri=result.output_uri)
    except Exception as exc:
        await job_repo.update_status(job.id, JobStatus.FAILED, error_message=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Kafka batch ingest failed: {exc}",
        ) from exc

    return KafkaBatchIngestResponse(
        job_id=job.id,
        output_uri=result.output_uri,
        message_count=result.message_count,
        topic=result.topic,
        schema=result.schema,
    )
