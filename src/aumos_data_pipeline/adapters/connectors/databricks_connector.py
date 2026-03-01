"""Databricks SQL connector for aumos-data-pipeline (GAP-115).

Ingests data from Databricks via SQL query using the Databricks SQL connector.
Credentials from aumos-secrets-vault (server hostname, HTTP path, token).
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


class DatabricksConnector:
    """Ingests data from Databricks SQL endpoints via SQL query.

    Uses databricks-sql-connector for query execution. Credentials are
    always from aumos-secrets-vault (server_hostname, http_path, access_token).

    Args:
        credentials_secret_id: Secret ID for Databricks connection credentials.
        secrets_client: Secrets vault client.
        catalog: Optional Unity Catalog name.
        schema: Optional schema/database name.
    """

    def __init__(
        self,
        credentials_secret_id: str,
        secrets_client: SecretsClientProtocol,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> None:
        """Initialize the Databricks SQL connector.

        Args:
            credentials_secret_id: Secret ID for Databricks credentials.
            secrets_client: Secrets vault client.
            catalog: Unity Catalog name (optional).
            schema: Schema name (optional).
        """
        self._credentials_secret_id = credentials_secret_id
        self._secrets_client = secrets_client
        self._catalog = catalog
        self._schema = schema

    async def ingest(
        self,
        query: str,
        staging_storage: StorageProtocol,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
    ) -> "IngestResult":
        """Execute Databricks SQL query and stage result as Parquet.

        Args:
            query: SQL SELECT query to execute on Databricks SQL endpoint.
            staging_storage: Storage adapter for Parquet staging.
            tenant_id: Owning tenant UUID.
            job_id: Pipeline job UUID.

        Returns:
            IngestResult with output URI, row count, and column schema.
        """
        from aumos_data_pipeline.adapters.connectors.snowflake_connector import IngestResult

        credentials = await self._secrets_client.get(self._credentials_secret_id)
        logger.info(
            "databricks_ingest_started",
            catalog=self._catalog,
            schema=self._schema,
            job_id=str(job_id),
        )

        try:
            from databricks import sql as dbsql  # type: ignore[import]

            def _run_query() -> pd.DataFrame:
                conn = dbsql.connect(
                    server_hostname=credentials["server_hostname"],
                    http_path=credentials["http_path"],
                    access_token=credentials["access_token"],
                    catalog=self._catalog,
                    schema=self._schema,
                )
                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    return cursor.fetchall_arrow().to_pandas()
                finally:
                    conn.close()

            df = await asyncio.to_thread(_run_query)
        except ImportError:
            logger.warning(
                "databricks_connector_not_installed",
                message="databricks-sql-connector not installed; using empty DataFrame",
            )
            df = pd.DataFrame()

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        key = f"dpl-staging/{tenant_id}/{job_id}/databricks_ingest.parquet"
        uri = await staging_storage.upload(buf.read(), key)

        logger.info(
            "databricks_ingest_completed",
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
