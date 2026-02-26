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
    DataProfileResponse,
    IngestRequest,
    IngestResponse,
    JobStatusResponse,
    OrchestrateRequest,
    OrchestrateResponse,
    PipelineJobResponse,
    ProfileRequest,
    ProfileResponse,
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
