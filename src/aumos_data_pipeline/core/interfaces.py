"""Protocol interfaces for the data pipeline service.

These Protocols define the contracts that all adapters must implement.
Services depend on these interfaces, never on concrete adapter classes,
ensuring testability and easy swapping of implementations.
"""

import uuid
from typing import Any, AsyncIterator, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class IngestorProtocol(Protocol):
    """Contract for data source connectors.

    Implementations ingest data from a specific source type
    (PostgreSQL, S3, CSV, REST API, streaming) and return
    a pandas DataFrame or async iterator of chunks.
    """

    async def ingest(
        self,
        source_config: dict[str, Any],
        chunk_size: int = 10_000,
    ) -> AsyncIterator[pd.DataFrame]:
        """Ingest data from the configured source.

        Args:
            source_config: Source-specific configuration (connection string, bucket, etc.)
            chunk_size: Number of rows per chunk for streaming ingest.

        Yields:
            DataFrame chunks of at most chunk_size rows.
        """
        ...

    async def validate_connection(self, source_config: dict[str, Any]) -> bool:
        """Test that the source is reachable and credentials are valid.

        Args:
            source_config: Source-specific configuration.

        Returns:
            True if connection succeeds, False otherwise.
        """
        ...


@runtime_checkable
class ProfilerProtocol(Protocol):
    """Contract for data profiling engines.

    Implementations compute statistical summaries, detect column types,
    identify missing values, and find distribution shapes.
    """

    async def profile(
        self,
        dataframe: pd.DataFrame,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Generate a statistical profile of the given DataFrame.

        Args:
            dataframe: The dataset to profile.
            job_id: Pipeline job ID for correlation.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: column_profiles, row_count, missing_values,
            distributions, outliers.
        """
        ...


@runtime_checkable
class CleanerProtocol(Protocol):
    """Contract for data cleaning implementations.

    Implementations perform deduplication, normalization, imputation,
    categorical encoding, and outlier handling.
    """

    async def clean(
        self,
        dataframe: pd.DataFrame,
        cleaning_config: dict[str, Any],
    ) -> pd.DataFrame:
        """Clean and normalize the given DataFrame.

        Args:
            dataframe: Raw dataset to clean.
            cleaning_config: Cleaning parameters (imputation strategy, encoding, etc.)

        Returns:
            Cleaned DataFrame with the same schema (or enriched via encoding).
        """
        ...


@runtime_checkable
class TransformerProtocol(Protocol):
    """Contract for data transformation and feature engineering.

    Implementations apply feature engineering, schema mapping,
    and format conversions (CSV to Parquet, JSON to Arrow, etc.).
    """

    async def transform(
        self,
        dataframe: pd.DataFrame,
        transform_config: dict[str, Any],
    ) -> pd.DataFrame:
        """Apply feature engineering and transformations.

        Args:
            dataframe: Cleaned dataset to transform.
            transform_config: Transformation parameters (scaling, encoding, feature ops).

        Returns:
            Transformed DataFrame ready for downstream AI workloads.
        """
        ...

    async def convert_format(
        self,
        input_uri: str,
        output_uri: str,
        target_format: str,
        compression: str = "snappy",
    ) -> str:
        """Convert a dataset between file formats.

        Args:
            input_uri: Source file URI (s3://, file://, etc.)
            output_uri: Destination file URI.
            target_format: Target format ("parquet", "arrow", "csv", "json").
            compression: Compression codec for parquet output.

        Returns:
            URI of the converted output file.
        """
        ...


@runtime_checkable
class QualityGateProtocol(Protocol):
    """Contract for data quality validation.

    Implementations apply expectation suites (Great Expectations or custom)
    and compute a quality score for the dataset.
    """

    async def validate(
        self,
        dataframe: pd.DataFrame,
        expectation_suite: str,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Validate the dataset against an expectation suite.

        Args:
            dataframe: Dataset to validate.
            expectation_suite: Name of the GE expectation suite.
            job_id: Pipeline job ID for correlation.
            tenant_id: Tenant context (suite stored per-tenant in MinIO).

        Returns:
            Dict with keys: passed (bool), quality_score (float), results (dict).
        """
        ...


@runtime_checkable
class VersionerProtocol(Protocol):
    """Contract for data versioning adapters.

    Implementations track dataset versions for reproducibility
    using DVC or similar tools.
    """

    async def commit_version(
        self,
        dataset_uri: str,
        metadata: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> str:
        """Commit a dataset version and return the version hash.

        Args:
            dataset_uri: URI of the dataset artifact in MinIO/S3.
            metadata: Version metadata (job_id, schema, row_count, etc.)
            tenant_id: Tenant context for namespacing.

        Returns:
            DVC commit hash (version identifier).
        """
        ...

    async def get_version(
        self,
        version_hash: str,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Retrieve metadata for a specific data version.

        Args:
            version_hash: DVC commit hash.
            tenant_id: Tenant context.

        Returns:
            Version metadata dict.
        """
        ...
