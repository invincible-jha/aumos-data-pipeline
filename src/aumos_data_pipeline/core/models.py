"""SQLAlchemy ORM models for the data pipeline service.

All models extend AumOSModel which provides:
  - id: UUID primary key
  - tenant_id: UUID (RLS enforced via aumos-common)
  - created_at: datetime
  - updated_at: datetime
"""

import enum
import uuid

from sqlalchemy import Boolean, Decimal, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from aumos_common.database import AumOSModel


class JobType(str, enum.Enum):
    """Types of pipeline jobs."""

    INGEST = "ingest"
    PROFILE = "profile"
    CLEAN = "clean"
    TRANSFORM = "transform"
    ORCHESTRATE = "orchestrate"


class JobStatus(str, enum.Enum):
    """Pipeline job execution states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineJob(AumOSModel):
    """Represents a single pipeline execution job.

    Tracks the full lifecycle of a data pipeline run — from initial submission
    through ingestion, profiling, cleaning, transformation, and quality gating.

    Table: dpl_pipeline_jobs
    """

    __tablename__ = "dpl_pipeline_jobs"

    job_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=JobStatus.PENDING, index=True)

    # Source and destination configuration stored as JSONB for flexibility
    source_config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    destination_config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Dataset shape metrics (populated after ingestion/profiling)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    column_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Quality score from Great Expectations (0.0–1.0)
    quality_score: Mapped[float | None] = mapped_column(Decimal(precision=5, scale=4), nullable=True)

    # DVC data version hash for reproducibility
    data_version: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # URI of the output artifact in MinIO/S3
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Error details if the job failed
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    profiles: Mapped[list["DataProfile"]] = relationship(
        "DataProfile", back_populates="job", cascade="all, delete-orphan"
    )
    quality_checks: Mapped[list["QualityCheck"]] = relationship(
        "QualityCheck", back_populates="job", cascade="all, delete-orphan"
    )


class DataProfile(AumOSModel):
    """Statistical profile of a dataset produced by the profiling stage.

    Stores column-level statistics, missing value counts, distributions,
    and detected outliers. Used by downstream services to understand
    data characteristics before synthesis or training.

    Table: dpl_data_profiles
    """

    __tablename__ = "dpl_data_profiles"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("dpl_pipeline_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Per-column statistics: {col_name: {dtype, min, max, mean, std, cardinality, ...}}
    column_profiles: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Missing values per column: {col_name: {count, pct}}
    missing_values: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Distribution histograms: {col_name: {bins: [...], counts: [...]}}
    distributions: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Detected outliers: {col_name: {method, threshold, count, indices: [...]}}
    outliers: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Relationship
    job: Mapped["PipelineJob"] = relationship("PipelineJob", back_populates="profiles")


class QualityCheck(AumOSModel):
    """Result of a Great Expectations quality gate run.

    Captures which expectation suite was applied, the full results JSON,
    and whether the dataset passed the quality threshold.

    Table: dpl_quality_checks
    """

    __tablename__ = "dpl_quality_checks"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("dpl_pipeline_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Name of the Great Expectations expectation suite applied
    expectation_suite: Mapped[str] = mapped_column(String(255), nullable=False)

    # Full GE validation results JSON
    results: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # True if quality_score >= threshold
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Relationship
    job: Mapped["PipelineJob"] = relationship("PipelineJob", back_populates="quality_checks")
