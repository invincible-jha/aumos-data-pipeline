"""Protocol interfaces for the data pipeline service.

These Protocols define the contracts that all adapters must implement.
Services depend on these interfaces, never on concrete adapter classes,
ensuring testability and easy swapping of implementations.
"""

import uuid
from typing import Any, AsyncIterator, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class DeidentifierProtocol(Protocol):
    """Contract for PII de-identification adapters.

    Implementations mask or redact PII fields in DataFrames using
    configurable strategies (hash, redact, pseudonymize, generalize).
    """

    async def deidentify(
        self,
        dataframe: pd.DataFrame,
        deidentification_config: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Apply PII masking to the DataFrame.

        Args:
            dataframe: Input dataset that may contain PII.
            deidentification_config: Masking configuration per column.
            job_id: Pipeline job ID for audit correlation.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: dataframe, audit_log, pii_columns_detected, risk_score.
        """
        ...

    async def detect_pii(
        self,
        dataframe: pd.DataFrame,
        sample_size: int = 500,
    ) -> dict[str, list[str]]:
        """Scan a DataFrame for columns containing PII.

        Args:
            dataframe: Dataset to scan.
            sample_size: Number of rows to sample per column.

        Returns:
            Dict mapping column name to list of detected PII entity types.
        """
        ...


@runtime_checkable
class SamplingEngineProtocol(Protocol):
    """Contract for data sampling strategy adapters.

    Implementations support random, stratified, systematic, importance,
    and reservoir sampling, plus statistical sample size calculation.
    """

    async def sample(
        self,
        dataframe: pd.DataFrame,
        sampling_config: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Sample the DataFrame using the configured strategy.

        Args:
            dataframe: Input dataset to sample.
            sampling_config: Strategy parameters (strategy, sample_size, seed, etc.)
            job_id: Pipeline job ID.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: dataframe, strategy, original_rows, sampled_rows,
            sampling_ratio, bias_report.
        """
        ...

    async def calculate_sample_size(
        self,
        population_size: int,
        margin_of_error: float = 0.05,
        confidence_level: float = 0.95,
        proportion: float = 0.5,
    ) -> dict[str, Any]:
        """Compute the minimum statistically valid sample size.

        Args:
            population_size: Total population N.
            margin_of_error: Desired margin of error (default 0.05).
            confidence_level: Confidence level (0.90, 0.95, or 0.99).
            proportion: Expected proportion for variance estimation.

        Returns:
            Dict with keys: sample_size, confidence_level, margin_of_error,
            proportion, sampling_ratio.
        """
        ...


@runtime_checkable
class IncrementalLoaderProtocol(Protocol):
    """Contract for incremental (delta) data loading adapters.

    Implementations track high watermarks per source table and apply
    CDC-based upsert logic to staging datasets.
    """

    async def load_delta(
        self,
        source_df: pd.DataFrame,
        existing_df: pd.DataFrame | None,
        load_config: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Compute and apply the delta between source and staging.

        Args:
            source_df: Full or recent extract from the source system.
            existing_df: Current staging dataset (None for first load).
            load_config: CDC configuration (watermark_column, cdc_mode, primary_key, etc.)
            job_id: Pipeline job ID.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: dataframe, inserted_rows, updated_rows, deleted_rows,
            new_watermark, checkpoint_key.
        """
        ...

    async def get_watermark(
        self,
        table_name: str,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Retrieve the current high watermark for a source table.

        Args:
            table_name: Logical source table name.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: checkpoint_key, watermark, last_updated.
        """
        ...

    async def reset_watermark(
        self,
        table_name: str,
        tenant_id: uuid.UUID,
    ) -> None:
        """Reset the watermark, forcing a full reload on the next run.

        Args:
            table_name: Logical source table name.
            tenant_id: Tenant context.
        """
        ...


@runtime_checkable
class DataQualityMonitorProtocol(Protocol):
    """Contract for data quality SLO monitoring adapters.

    Implementations evaluate quality rules against DataFrames, track metric
    history, and dispatch alerts when SLOs are violated.
    """

    async def evaluate_rules(
        self,
        dataframe: pd.DataFrame,
        rules: list[dict[str, Any]],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Evaluate a list of quality rules against the DataFrame.

        Args:
            dataframe: Dataset to evaluate.
            rules: Quality rule definitions (dimension, column, threshold, operator).
            job_id: Pipeline job ID.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: passed, quality_score, rule_results, violations,
            alerts_dispatched.
        """
        ...

    async def aggregate_dashboard_data(
        self,
        tenant_id: uuid.UUID,
        lookback_entries: int = 30,
    ) -> dict[str, Any]:
        """Aggregate quality metric trends for dashboard display.

        Args:
            tenant_id: Tenant context.
            lookback_entries: Number of historical entries per metric.

        Returns:
            Dict of metric trend data per metric key.
        """
        ...


@runtime_checkable
class DataLineageTrackerProtocol(Protocol):
    """Contract for data lineage tracking adapters.

    Implementations record source-to-target field mappings, build a DAG
    of data flows, and provide impact analysis and field tracing APIs.
    """

    async def record_transformation(
        self,
        step_name: str,
        input_uris: list[str],
        output_uri: str,
        field_mappings: dict[str, list[str]],
        metadata: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> str:
        """Record a transformation step in the lineage graph.

        Args:
            step_name: Human-readable name of the transformation.
            input_uris: List of input dataset URIs.
            output_uri: URI of the output dataset.
            field_mappings: Output-to-input field mapping {out_col: [in_cols]}.
            metadata: Additional context (row_count, schema, version).
            job_id: Pipeline job ID.
            tenant_id: Tenant context.

        Returns:
            Lineage edge ID (UUID string).
        """
        ...

    async def trace_field_origin(
        self,
        output_uri: str,
        field_name: str,
        tenant_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        """Trace a field back through the lineage graph to its origins.

        Args:
            output_uri: URI of the dataset containing the field.
            field_name: Name of the output field to trace.
            tenant_id: Tenant context.

        Returns:
            List of lineage hops as dicts.
        """
        ...

    async def analyze_impact(
        self,
        source_uri: str,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Identify downstream datasets and fields affected by a source change.

        Args:
            source_uri: URI of the dataset that is changing.
            tenant_id: Tenant context.

        Returns:
            Dict with keys: affected_datasets, affected_fields, impact_depth.
        """
        ...

    async def export_lineage_graph(
        self,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Export the full lineage graph as a visualization payload.

        Args:
            tenant_id: Tenant context.

        Returns:
            Dict with nodes and edges for graph rendering.
        """
        ...


@runtime_checkable
class SchedulingAdapterProtocol(Protocol):
    """Contract for orchestration scheduling adapters.

    Implementations generate DAG/workflow definitions for Airflow and
    Temporal, validate cron expressions, and track pipeline run status.
    """

    async def generate_airflow_dag(
        self,
        pipeline_config: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> str:
        """Generate an Airflow DAG Python file string.

        Args:
            pipeline_config: Pipeline definition dict.
            tenant_id: Tenant context.

        Returns:
            Python source code string defining the Airflow DAG.
        """
        ...

    async def generate_temporal_workflow(
        self,
        pipeline_config: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Generate a Temporal workflow definition dict.

        Args:
            pipeline_config: Pipeline definition dict.
            tenant_id: Tenant context.

        Returns:
            Temporal workflow spec dict.
        """
        ...

    async def validate_cron(self, cron_expression: str) -> dict[str, Any]:
        """Validate a cron expression and compute next run times.

        Args:
            cron_expression: Standard 5-field cron string or special alias.

        Returns:
            Dict with keys: valid, expression, next_runs, description, error.
        """
        ...

    async def track_run(
        self,
        pipeline_id: str,
        run_id: str,
        status: str,
        metadata: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Record or update a pipeline run's status.

        Args:
            pipeline_id: Logical pipeline identifier.
            run_id: Unique run identifier.
            status: Run status string.
            metadata: Additional context for the run.
            tenant_id: Tenant context.

        Returns:
            Updated run record dict.
        """
        ...


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
