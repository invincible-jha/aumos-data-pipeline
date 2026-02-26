"""PostgreSQL source connector.

Ingests data from a PostgreSQL table or arbitrary query using asyncpg
with streaming cursor support for large tables.
"""

from typing import Any, AsyncIterator

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import IngestorProtocol

logger = get_logger(__name__)


class PostgresConnector:
    """Ingests data from a PostgreSQL database.

    Implements IngestorProtocol. Supports table ingestion and arbitrary
    SQL queries. Uses streaming cursors to handle large tables without
    loading everything into memory at once.

    Expected source_config keys:
        connection_string: asyncpg connection URI
        table: Table name (exclusive with query)
        query: Raw SQL SELECT query (exclusive with table)
        schema: PostgreSQL schema (default: public)
        columns: List of columns to select (default: all)
    """

    async def ingest(
        self,
        source_config: dict[str, Any],
        chunk_size: int = 10_000,
    ) -> AsyncIterator[pd.DataFrame]:
        """Stream data from PostgreSQL in chunks.

        Args:
            source_config: PostgreSQL connection and query configuration.
            chunk_size: Rows per chunk.

        Yields:
            DataFrame chunks.
        """
        connection_string = source_config["connection_string"]
        schema = source_config.get("schema", "public")

        if "query" in source_config:
            query = source_config["query"]
        elif "table" in source_config:
            table = source_config["table"]
            columns = source_config.get("columns", ["*"])
            col_str = ", ".join(columns)
            query = f'SELECT {col_str} FROM "{schema}"."{table}"'  # noqa: S608
        else:
            raise ValueError("source_config must include 'table' or 'query'")

        logger.info("PostgreSQL ingestion starting", query_preview=query[:100])

        engine = create_async_engine(connection_string, echo=False)
        try:
            async with engine.connect() as conn:
                result = await conn.stream(sa.text(query))
                chunk_rows: list[dict[str, Any]] = []
                columns_list: list[str] = []

                async for row in result:
                    if not columns_list:
                        columns_list = list(result.keys())
                    chunk_rows.append(dict(zip(columns_list, row, strict=True)))

                    if len(chunk_rows) >= chunk_size:
                        yield pd.DataFrame(chunk_rows, columns=columns_list)
                        chunk_rows = []

                if chunk_rows:
                    yield pd.DataFrame(chunk_rows, columns=columns_list)
        finally:
            await engine.dispose()

    async def validate_connection(self, source_config: dict[str, Any]) -> bool:
        """Test that the PostgreSQL connection is reachable.

        Args:
            source_config: Must include 'connection_string'.

        Returns:
            True if connection succeeds.
        """
        try:
            engine = create_async_engine(source_config["connection_string"], echo=False)
            async with engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1"))
            await engine.dispose()
            return True
        except Exception as exc:
            logger.warning("PostgreSQL connection validation failed", error=str(exc))
            return False
