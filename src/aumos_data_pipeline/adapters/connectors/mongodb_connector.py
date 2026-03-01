"""MongoDB source connector for aumos-data-pipeline (GAP-115).

Ingests documents from a MongoDB collection using an async Motor client.
Serializes ObjectId fields to strings. Credentials from aumos-secrets-vault.
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


class MongoDBConnector:
    """Ingests documents from a MongoDB collection.

    Uses Motor (async MongoDB client). Serializes ObjectId to string before
    converting to Parquet. Credentials from aumos-secrets-vault.

    Args:
        connection_string_secret_id: Secret ID for the MongoDB connection string.
        database: MongoDB database name.
        collection: Collection name to ingest from.
        secrets_client: Secrets vault client.
    """

    def __init__(
        self,
        connection_string_secret_id: str,
        database: str,
        collection: str,
        secrets_client: SecretsClientProtocol,
    ) -> None:
        """Initialize the MongoDB connector.

        Args:
            connection_string_secret_id: Secret ID for connection string.
            database: MongoDB database name.
            collection: Collection name.
            secrets_client: Secrets vault client.
        """
        self._connection_string_secret_id = connection_string_secret_id
        self._database = database
        self._collection = collection
        self._secrets_client = secrets_client

    async def _get_connection_string(self) -> str:
        """Retrieve connection string from secrets vault.

        Returns:
            MongoDB connection string.
        """
        secret = await self._secrets_client.get(self._connection_string_secret_id)
        return secret["connection_string"]

    async def ingest(
        self,
        query_filter: dict[str, Any],
        staging_storage: StorageProtocol,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
        projection: dict[str, int] | None = None,
        limit: int = 0,
    ) -> "IngestResult":
        """Fetch documents matching filter and stage to MinIO as Parquet.

        Args:
            query_filter: MongoDB query filter document.
            staging_storage: Storage adapter for Parquet staging.
            tenant_id: Owning tenant UUID.
            job_id: Pipeline job UUID.
            projection: Optional field projection dict.
            limit: Maximum documents to retrieve (0 = all).

        Returns:
            IngestResult with output URI, row count, and column schema.
        """
        from aumos_data_pipeline.adapters.connectors.snowflake_connector import IngestResult

        logger.info(
            "mongodb_ingest_started",
            database=self._database,
            collection=self._collection,
            job_id=str(job_id),
        )

        try:
            from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore[import]

            connection_string = await self._get_connection_string()
            client: AsyncIOMotorClient = AsyncIOMotorClient(connection_string)
            coll = client[self._database][self._collection]

            cursor = coll.find(query_filter, projection=projection)
            if limit > 0:
                cursor = cursor.limit(limit)

            documents = await cursor.to_list(length=None)
            client.close()

            df = pd.DataFrame(documents)
            if "_id" in df.columns:
                df["_id"] = df["_id"].astype(str)

        except ImportError:
            logger.warning(
                "motor_not_installed",
                message="motor not installed; using empty DataFrame",
            )
            df = pd.DataFrame()

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        key = f"dpl-staging/{tenant_id}/{job_id}/mongodb_ingest.parquet"
        uri = await staging_storage.upload(buf.read(), key)

        logger.info(
            "mongodb_ingest_completed",
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
