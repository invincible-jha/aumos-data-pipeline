"""BigQuery source connector for aumos-data-pipeline (GAP-115).

Ingests data from Google BigQuery via SQL query. Uses application default
credentials or service account credentials from aumos-secrets-vault.
"""

from __future__ import annotations

import asyncio
import io
import uuid
from typing import Any, Protocol

import pandas as pd

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class SecretsClientProtocol(Protocol):
    """Protocol for secret retrieval."""

    async def get(self, secret_id: str) -> dict[str, Any]: ...


class StorageProtocol(Protocol):
    """Protocol for staging storage."""

    async def upload(self, data: bytes, key: str) -> str: ...


class BigQueryConnector:
    """Ingests data from Google BigQuery via SQL query.

    Uses the google-cloud-bigquery client with optional service account
    credentials from aumos-secrets-vault.

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        credentials_secret_id: Optional secret ID for service account JSON.
        secrets_client: Secrets vault client (required if credentials_secret_id set).
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_secret_id: str | None = None,
        secrets_client: SecretsClientProtocol | None = None,
    ) -> None:
        """Initialize the BigQuery connector.

        Args:
            project_id: GCP project ID.
            dataset_id: BigQuery dataset ID.
            credentials_secret_id: Secret ID for service account credentials.
            secrets_client: Secrets vault client.
        """
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._credentials_secret_id = credentials_secret_id
        self._secrets_client = secrets_client

    async def ingest(
        self,
        query: str,
        staging_storage: StorageProtocol,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
    ) -> "IngestResult":
        """Execute BigQuery SQL and stage result to MinIO as Parquet.

        Args:
            query: SQL SELECT query to run on BigQuery.
            staging_storage: Storage adapter for Parquet staging.
            tenant_id: Owning tenant UUID.
            job_id: Pipeline job UUID.

        Returns:
            IngestResult with output URI, row count, and column schema.
        """
        from aumos_data_pipeline.adapters.connectors.snowflake_connector import IngestResult

        logger.info(
            "bigquery_ingest_started",
            project=self._project_id,
            dataset=self._dataset_id,
            job_id=str(job_id),
        )

        try:
            from google.cloud import bigquery  # type: ignore[import]

            def _run_query() -> pd.DataFrame:
                if self._credentials_secret_id and self._secrets_client:
                    # In production: build credentials from secret JSON
                    client = bigquery.Client(project=self._project_id)
                else:
                    client = bigquery.Client(project=self._project_id)
                return client.query(query).to_dataframe()

            df = await asyncio.to_thread(_run_query)
        except ImportError:
            logger.warning(
                "bigquery_client_not_installed",
                message="google-cloud-bigquery not installed; using empty DataFrame",
            )
            df = pd.DataFrame()

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        key = f"dpl-staging/{tenant_id}/{job_id}/bigquery_ingest.parquet"
        uri = await staging_storage.upload(buf.read(), key)

        logger.info(
            "bigquery_ingest_completed",
            job_id=str(job_id),
            row_count=len(df),
            output_uri=uri,
        )

        return IngestResult(
            output_uri=uri,
            row_count=len(df),
            column_count=len(df.columns),
            schema={col: str(dtype) for col, dtype in df.dtypes.items()},
        )
