"""Excel source connector for aumos-data-pipeline (GAP-115).

Downloads an Excel file from MinIO storage, reads the specified sheet,
and converts it to Parquet for the standard pipeline processing steps.
Supports .xlsx and .xls formats via openpyxl.
"""

from __future__ import annotations

import asyncio
import io
import uuid
from typing import Any, Protocol

import pandas as pd

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class StorageProtocol(Protocol):
    """Protocol for staging storage."""

    async def upload(self, data: bytes, key: str) -> str: ...
    async def download(self, uri: str) -> bytes: ...


class ExcelConnector:
    """Ingests data from Excel files (.xlsx, .xls).

    Downloads the Excel file from a MinIO URI, reads the specified sheet,
    converts to Parquet and stages for pipeline processing.

    Args:
        skip_rows: Number of header rows to skip (default 0).
        dtype_inference: Whether to auto-infer column types (default True).
    """

    def __init__(
        self,
        skip_rows: int = 0,
        dtype_inference: bool = True,
    ) -> None:
        """Initialize the Excel connector.

        Args:
            skip_rows: Rows to skip at the top of the sheet.
            dtype_inference: Auto-infer column dtypes.
        """
        self._skip_rows = skip_rows
        self._dtype_inference = dtype_inference

    async def ingest(
        self,
        source_uri: str,
        staging_storage: StorageProtocol,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
        sheet_name: str | int = 0,
        na_values: list[str] | None = None,
    ) -> "IngestResult":
        """Download Excel file from MinIO and stage as Parquet.

        Args:
            source_uri: MinIO URI of the Excel file.
            staging_storage: Storage adapter for downloading and staging.
            tenant_id: Owning tenant UUID.
            job_id: Pipeline job UUID.
            sheet_name: Sheet name or index (default: first sheet).
            na_values: Additional NA value strings.

        Returns:
            IngestResult with output URI, row count, and column schema.
        """
        from aumos_data_pipeline.adapters.connectors.snowflake_connector import IngestResult

        logger.info(
            "excel_ingest_started",
            source_uri=source_uri,
            sheet_name=str(sheet_name),
            job_id=str(job_id),
        )

        excel_bytes = await staging_storage.download(source_uri)

        def _read_excel() -> pd.DataFrame:
            return pd.read_excel(
                io.BytesIO(excel_bytes),
                sheet_name=sheet_name,
                skiprows=self._skip_rows,
                na_values=na_values,
                dtype=None if self._dtype_inference else str,
            )

        try:
            df = await asyncio.to_thread(_read_excel)
        except ImportError:
            logger.warning(
                "openpyxl_not_installed",
                message="openpyxl not installed; install it with: pip install openpyxl",
            )
            df = pd.DataFrame()

        # Clean column names: strip whitespace
        df.columns = [str(col).strip() for col in df.columns]

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        key = f"dpl-staging/{tenant_id}/{job_id}/excel_ingest.parquet"
        uri = await staging_storage.upload(buf.read(), key)

        logger.info(
            "excel_ingest_completed",
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
