"""Pydantic request and response models for the data pipeline API.

All API schemas are strictly typed. Enums match the ORM model enums
to avoid divergence between API contract and persistence layer.
"""

import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator

from aumos_data_pipeline.core.models import JobStatus, JobType


# ---------------------------------------------------------------------------
# Shared / Base
# ---------------------------------------------------------------------------


class PipelineJobResponse(BaseModel):
    """Response schema for a pipeline job record."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    job_type: JobType
    status: JobStatus
    source_config: dict[str, Any]
    destination_config: dict[str, Any]
    row_count: int | None = None
    column_count: int | None = None
    quality_score: float | None = None
    data_version: str | None = None
    output_uri: str | None = None
    error_message: str | None = None

    model_config = {"from_attributes": True}


class DataProfileResponse(BaseModel):
    """Response schema for a data profile record."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    job_id: uuid.UUID
    row_count: int
    column_profiles: dict[str, Any]
    missing_values: dict[str, Any]
    distributions: dict[str, Any]
    outliers: dict[str, Any]

    model_config = {"from_attributes": True}


class QualityCheckResponse(BaseModel):
    """Response schema for a quality check record."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    job_id: uuid.UUID
    expectation_suite: str
    results: dict[str, Any]
    passed: bool

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


class IngestRequest(BaseModel):
    """Request to ingest data from a source.

    The source_config must include 'source_type' to select the connector:
    - 'postgres': PostgreSQL table/query ingestion
    - 's3': S3/MinIO object storage
    - 'csv': Local CSV or Parquet file
    - 'api': REST API endpoint
    - 'stream': Kafka streaming source
    """

    source_config: dict[str, Any] = Field(
        ...,
        description="Source configuration including 'source_type'",
        examples=[{"source_type": "postgres", "table": "customers", "connection_string": "..."}],
    )
    destination_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Output configuration (path, format, compression)",
    )

    @field_validator("source_config")
    @classmethod
    def source_type_required(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Ensure source_type is present in source_config."""
        if "source_type" not in value:
            raise ValueError("source_config must include 'source_type'")
        return value


class IngestResponse(BaseModel):
    """Response after submitting an ingestion request."""

    job: PipelineJobResponse


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------


class ProfileRequest(BaseModel):
    """Request to profile a dataset."""

    input_uri: str = Field(..., description="MinIO/S3 URI of the dataset to profile")
    source_job_id: uuid.UUID | None = Field(
        default=None,
        description="Optional ID of the ingestion job that produced this dataset",
    )


class ProfileResponse(BaseModel):
    """Response after submitting a profiling request."""

    job: PipelineJobResponse
    profile: DataProfileResponse


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------


class CleanRequest(BaseModel):
    """Request to clean a dataset.

    Cleaning config options:
    - dedup_subset: columns to use for deduplication (empty = all columns)
    - imputation_strategy: 'mean' | 'median' | 'mode' | 'drop' | 'none'
    - outlier_handling: 'clip' | 'remove' | 'none'
    - normalize: list of columns to normalize
    - encoding: {col: 'onehot' | 'label' | 'ordinal'}
    """

    input_uri: str = Field(..., description="MinIO/S3 URI of the dataset to clean")
    cleaning_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Cleaning parameters",
        examples=[
            {
                "dedup_subset": ["id"],
                "imputation_strategy": "median",
                "outlier_handling": "clip",
                "normalize": ["age", "income"],
            }
        ],
    )
    destination_config: dict[str, Any] = Field(default_factory=dict)


class CleanResponse(BaseModel):
    """Response after submitting a cleaning request."""

    job: PipelineJobResponse


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


class TransformRequest(BaseModel):
    """Request to apply feature engineering to a dataset."""

    input_uri: str = Field(..., description="MinIO/S3 URI of the cleaned dataset")
    transform_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Transformation parameters",
        examples=[
            {
                "scale": {"age": "zscore", "income": "minmax"},
                "encode": {"gender": "onehot", "category": "label"},
                "drop_cols": ["id", "raw_timestamp"],
                "target_format": "parquet",
            }
        ],
    )
    destination_config: dict[str, Any] = Field(default_factory=dict)


class TransformResponse(BaseModel):
    """Response after submitting a transformation request."""

    job: PipelineJobResponse


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


class OrchestrateRequest(BaseModel):
    """Request to run the full pipeline end-to-end.

    Controls which stages to execute via skip_* flags.
    At minimum, either source_config (for ingestion) or input_uri must be provided.
    """

    source_config: dict[str, Any] | None = Field(
        default=None,
        description="Source configuration for ingestion stage",
    )
    input_uri: str | None = Field(
        default=None,
        description="Pre-ingested dataset URI (skip ingestion stage)",
    )
    skip_ingest: bool = Field(default=False)
    skip_profile: bool = Field(default=False)
    skip_clean: bool = Field(default=False)
    skip_transform: bool = Field(default=False)
    cleaning_config: dict[str, Any] = Field(default_factory=dict)
    transform_config: dict[str, Any] = Field(default_factory=dict)
    destination_config: dict[str, Any] = Field(default_factory=dict)
    expectation_suite: str = Field(default="", description="GE expectation suite name (empty = skip quality gate)")
    version_output: bool = Field(default=True, description="DVC-version the final output")
    abort_on_quality_failure: bool = Field(default=True)

    @field_validator("source_config", "input_uri", mode="before")
    @classmethod
    def require_source_or_uri(cls, value: Any) -> Any:
        """Validated at model level — at least one source must be provided."""
        return value


class OrchestrateResponse(BaseModel):
    """Response after submitting a full pipeline orchestration."""

    job: PipelineJobResponse


# ---------------------------------------------------------------------------
# Job / Profile GET responses
# ---------------------------------------------------------------------------


class JobStatusResponse(BaseModel):
    """Response for GET /pipeline/jobs/{id}."""

    job: PipelineJobResponse
    quality_checks: list[QualityCheckResponse] = Field(default_factory=list)
