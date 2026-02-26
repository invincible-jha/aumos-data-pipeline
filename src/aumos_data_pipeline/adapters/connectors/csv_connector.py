"""CSV/Parquet file source connector.

Ingests data from local or mounted filesystem CSV and Parquet files.
Supports chunked reading for large files.
"""

from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd
import pyarrow.parquet as pq

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import IngestorProtocol

logger = get_logger(__name__)


class CsvConnector:
    """Ingests data from CSV and Parquet files.

    Implements IngestorProtocol. Supports local filesystem and mounted
    paths. Auto-detects format from file extension.

    Expected source_config keys:
        file_path: Absolute path to the file
        format: 'csv' | 'parquet' | 'auto' (default: auto)
        encoding: File encoding for CSV (default: utf-8)
        delimiter: CSV delimiter (default: ,)
        has_header: Whether CSV has header row (default: true)
        columns: List of columns to load (default: all)
    """

    async def ingest(
        self,
        source_config: dict[str, Any],
        chunk_size: int = 10_000,
    ) -> AsyncIterator[pd.DataFrame]:
        """Read a file in chunks.

        Args:
            source_config: File path and format configuration.
            chunk_size: Rows per chunk.

        Yields:
            DataFrame chunks.
        """
        file_path = Path(source_config["file_path"])

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_format = source_config.get("format", "auto")
        if file_format == "auto":
            suffix = file_path.suffix.lower()
            file_format = "parquet" if suffix in (".parquet", ".pq") else "csv"

        columns = source_config.get("columns") or None

        logger.info("File ingestion starting", file_path=str(file_path), format=file_format)

        if file_format == "parquet":
            table = pq.read_table(file_path, columns=columns)
            dataframe = table.to_pandas()
            for i in range(0, len(dataframe), chunk_size):
                yield dataframe.iloc[i : i + chunk_size].copy()
        else:
            # CSV chunked read
            csv_kwargs: dict[str, Any] = {
                "encoding": source_config.get("encoding", "utf-8"),
                "sep": source_config.get("delimiter", ","),
                "header": 0 if source_config.get("has_header", True) else None,
                "chunksize": chunk_size,
            }
            if columns:
                csv_kwargs["usecols"] = columns

            for chunk in pd.read_csv(file_path, **csv_kwargs):
                yield chunk

    async def validate_connection(self, source_config: dict[str, Any]) -> bool:
        """Test that the file path is accessible.

        Args:
            source_config: Must include 'file_path'.

        Returns:
            True if the file exists and is readable.
        """
        try:
            file_path = Path(source_config["file_path"])
            return file_path.exists() and file_path.is_file()
        except Exception as exc:
            logger.warning("CSV/file connection validation failed", error=str(exc))
            return False
