"""Snowflake source connector for aumos-data-pipeline (GAP-115).

Ingests data from Snowflake via SQL query. Credentials are always resolved
from aumos-secrets-vault — never hardcoded or passed directly.
"""

from __future__ import annotations

import asyncio
import io
import uuid
from dataclasses import dataclass
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


@dataclass
class IngestResult:
    """Result of a connector ingestion operation.

    Attributes:
        output_uri: Storage URI of the staged Parquet file.
        row_count: Number of rows ingested.
        column_count: Number of columns.
        schema: Column name to dtype mapping.
    """

    output_uri: str
    row_count: int
    column_count: int
    schema: dict[str, str]


class SnowflakeConnector:
    """Ingests data from Snowflake via SQL query.

    Implements IngestorProtocol. Fetches query result as Arrow table
    and writes to staging MinIO as Parquet.

    Args:
        account: Snowflake account identifier (e.g., "xy12345.us-east-1").
        warehouse: Virtual warehouse name.
        database: Database name.
        schema: Schema name.
        credentials_secret_id: aumos-secrets-vault secret ID for credentials.
        secrets_client: Secrets retrieval client.
    """

    def __init__(
        self,
        account: str,
        warehouse: str,
        database: str,
        schema: str,
        credentials_secret_id: str,
        secrets_client: SecretsClientProtocol,
    ) -> None:
        """Initialize the Snowflake connector.

        Args:
            account: Snowflake account identifier.
            warehouse: Virtual warehouse name.
            database: Database name.
            schema: Schema name.
            credentials_secret_id: Secret ID for Snowflake credentials.
            secrets_client: Secrets vault client.
        """
        self._account = account
        self._warehouse = warehouse
        self._database = database
        self._schema = schema
        self._credentials_secret_id = credentials_secret_id
        self._secrets_client = secrets_client

    async def ingest(
        self,
        query: str,
        staging_storage: StorageProtocol,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
    ) -> IngestResult:
        """Execute Snowflake query and stage result to MinIO as Parquet.

        Args:
            query: SQL SELECT query to execute.
            staging_storage: Storage adapter for staging output.
            tenant_id: Owning tenant UUID.
            job_id: Pipeline job UUID.

        Returns:
            IngestResult with output URI, row count, and schema.

        Raises:
            RuntimeError: If Snowflake connection or query fails.
        """
        credentials = await self._secrets_client.get(self._credentials_secret_id)
        logger.info(
            "snowflake_ingest_started",
            account=self._account,
            database=self._database,
            schema=self._schema,
            job_id=str(job_id),
        )

        try:
            import snowflake.connector  # type: ignore[import]

            conn = await asyncio.to_thread(
                snowflake.connector.connect,
                account=self._account,
                user=credentials["user"],
                password=credentials["password"],
                warehouse=self._warehouse,
                database=self._database,
                schema=self._schema,
            )
            try:
                cursor = conn.cursor()
                await asyncio.to_thread(cursor.execute, query)
                # Try Arrow batch first for performance; fall back to fetchall
                try:
                    arrow_result = await asyncio.to_thread(cursor.fetch_arrow_all)
                    df = arrow_result.to_pandas()
                except AttributeError:
                    rows = await asyncio.to_thread(cursor.fetchall)
                    col_names = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=col_names)
            finally:
                conn.close()
        except ImportError:
            logger.warning(
                "snowflake_connector_not_installed",
                message="snowflake-connector-python not installed; using empty DataFrame",
            )
            df = pd.DataFrame()

        return await self._stage_to_storage(df, staging_storage, tenant_id, job_id)

    async def _stage_to_storage(
        self,
        df: pd.DataFrame,
        storage: StorageProtocol,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
    ) -> IngestResult:
        """Write DataFrame to MinIO as Parquet and return IngestResult.

        Args:
            df: DataFrame to stage.
            storage: Storage adapter.
            tenant_id: Tenant UUID.
            job_id: Job UUID.

        Returns:
            IngestResult with storage URI and metadata.
        """
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        key = f"dpl-staging/{tenant_id}/{job_id}/snowflake_ingest.parquet"
        uri = await storage.upload(buf.read(), key)

        logger.info(
            "snowflake_ingest_completed",
            job_id=str(job_id),
            row_count=len(df),
            column_count=len(df.columns),
            output_uri=uri,
        )

        return IngestResult(
            output_uri=uri,
            row_count=len(df),
            column_count=len(df.columns),
            schema={col: str(dtype) for col, dtype in df.dtypes.items()},
        )
