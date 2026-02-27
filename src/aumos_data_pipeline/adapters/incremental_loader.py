"""Incremental (delta) data loading adapter.

Implements Change Data Capture (CDC) patterns for efficient re-ingestion
of only new or changed rows from a source system. Tracks high watermarks
per source table in a checkpoint store and performs upsert logic on the
staging dataset.

Supported CDC modes:
- ``timestamp``: Uses a last-modified column to detect new/changed rows
- ``sequence``: Uses a monotonically increasing integer key
- ``soft_delete``: Detects deleted rows via a boolean is_deleted column
"""

import datetime
import json
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import IncrementalLoaderProtocol

logger = get_logger(__name__)

# Sentinel value stored when no prior watermark exists
_EPOCH = "1970-01-01T00:00:00+00:00"
# Default batch size for delta ingestion
_DEFAULT_BATCH_SIZE = 50_000


class IncrementalLoader:
    """Loads only new and changed rows from a source dataset.

    Implements IncrementalLoaderProtocol. Maintains a JSON checkpoint file
    per (tenant_id, table_name) pair that records the current high watermark.
    On each invocation it loads rows beyond the watermark, applies upsert
    logic against the existing staging DataFrame, and advances the checkpoint.

    Checkpoint store location is controlled by ``checkpoint_dir`` (defaults to
    a temporary directory in production).

    Usage::

        loader = IncrementalLoader(checkpoint_dir="/var/aumos/checkpoints")
        result = await loader.load_delta(
            source_df=full_source_df,
            existing_df=current_staging_df,
            load_config={
                "table_name": "crm_customers",
                "watermark_column": "updated_at",
                "cdc_mode": "timestamp",
                "primary_key": "customer_id",
                "soft_delete_column": "is_deleted",
                "batch_size": 10000,
            },
            job_id=job_id,
            tenant_id=tenant_id,
        )
    """

    def __init__(self, checkpoint_dir: str = "/tmp/aumos_checkpoints") -> None:
        """Initialize the incremental loader.

        Args:
            checkpoint_dir: Directory for storing watermark checkpoint files.
                Created if it does not exist.
        """
        self._checkpoint_dir = Path(checkpoint_dir)
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)

    async def load_delta(
        self,
        source_df: pd.DataFrame,
        existing_df: pd.DataFrame | None,
        load_config: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Compute and apply the delta between the source and the current staging dataset.

        Args:
            source_df: Full (or recent) extract from the source system.
            existing_df: Current staging dataset (None for first load).
            load_config: CDC configuration:
                - table_name: Logical name of the source table (str)
                - watermark_column: Column used for CDC (datetime or int)
                - cdc_mode: 'timestamp' | 'sequence' | 'soft_delete'
                - primary_key: Column(s) used as the upsert key (str or list[str])
                - soft_delete_column: Boolean column indicating deleted rows
                - batch_size: Max rows per delta batch (default 50_000)
            job_id: Pipeline job ID for audit logging.
            tenant_id: Tenant context (used to namespace checkpoints).

        Returns:
            Dict containing:
                - dataframe: Updated staging DataFrame after applying the delta
                - inserted_rows: Number of newly inserted rows
                - updated_rows: Number of updated rows
                - deleted_rows: Number of soft-deleted rows removed
                - new_watermark: The high watermark after this load
                - checkpoint_key: Checkpoint file key for this source table
        """
        table_name: str = load_config.get("table_name", "unknown_table")
        watermark_col: str = load_config.get("watermark_column", "updated_at")
        cdc_mode: str = load_config.get("cdc_mode", "timestamp")
        primary_key: str | list[str] = load_config.get("primary_key", "id")
        soft_delete_col: str = load_config.get("soft_delete_column", "is_deleted")
        batch_size: int = load_config.get("batch_size", _DEFAULT_BATCH_SIZE)

        if isinstance(primary_key, str):
            primary_key = [primary_key]

        checkpoint_key = self._build_checkpoint_key(tenant_id, table_name)
        current_watermark = self._load_watermark(checkpoint_key)

        logger.info(
            "Incremental load started",
            table_name=table_name,
            cdc_mode=cdc_mode,
            current_watermark=str(current_watermark),
            source_rows=len(source_df),
            job_id=str(job_id),
        )

        # Identify new/changed rows beyond the watermark
        delta_df = self._extract_delta(
            source_df=source_df,
            watermark_col=watermark_col,
            current_watermark=current_watermark,
            cdc_mode=cdc_mode,
        )

        if len(delta_df) == 0:
            logger.info(
                "No delta rows found — dataset is up to date",
                table_name=table_name,
                job_id=str(job_id),
            )
            return {
                "dataframe": existing_df if existing_df is not None else pd.DataFrame(),
                "inserted_rows": 0,
                "updated_rows": 0,
                "deleted_rows": 0,
                "new_watermark": str(current_watermark),
                "checkpoint_key": checkpoint_key,
            }

        # Process in batches to control memory usage
        delta_batches = self._split_into_batches(delta_df, batch_size)
        staging_df = existing_df.copy() if existing_df is not None else pd.DataFrame()
        total_inserted = 0
        total_updated = 0
        total_deleted = 0

        for batch_index, batch in enumerate(delta_batches):
            staging_df, inserted, updated, deleted = self._apply_upsert(
                staging_df=staging_df,
                delta_batch=batch,
                primary_key=primary_key,
                soft_delete_col=soft_delete_col if cdc_mode == "soft_delete" else "",
            )
            total_inserted += inserted
            total_updated += updated
            total_deleted += deleted

            logger.info(
                "Delta batch applied",
                batch_index=batch_index,
                batch_rows=len(batch),
                inserted=inserted,
                updated=updated,
                deleted=deleted,
                job_id=str(job_id),
            )

        # Advance the watermark
        new_watermark = self._compute_new_watermark(delta_df, watermark_col, cdc_mode)
        self._save_watermark(checkpoint_key, new_watermark)

        logger.info(
            "Incremental load completed",
            table_name=table_name,
            inserted=total_inserted,
            updated=total_updated,
            deleted=total_deleted,
            new_watermark=str(new_watermark),
            job_id=str(job_id),
        )

        return {
            "dataframe": staging_df,
            "inserted_rows": total_inserted,
            "updated_rows": total_updated,
            "deleted_rows": total_deleted,
            "new_watermark": str(new_watermark),
            "checkpoint_key": checkpoint_key,
        }

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
            Dict with checkpoint_key, watermark, and last_updated timestamp.
        """
        checkpoint_key = self._build_checkpoint_key(tenant_id, table_name)
        watermark = self._load_watermark(checkpoint_key)
        checkpoint_path = self._checkpoint_path(checkpoint_key)

        last_updated: str | None = None
        if checkpoint_path.exists():
            mtime = checkpoint_path.stat().st_mtime
            last_updated = datetime.datetime.fromtimestamp(
                mtime, tz=datetime.timezone.utc
            ).isoformat()

        return {
            "checkpoint_key": checkpoint_key,
            "watermark": str(watermark),
            "last_updated": last_updated,
        }

    async def reset_watermark(
        self,
        table_name: str,
        tenant_id: uuid.UUID,
    ) -> None:
        """Reset the watermark for a source table, forcing a full reload on the next run.

        Args:
            table_name: Logical source table name.
            tenant_id: Tenant context.
        """
        checkpoint_key = self._build_checkpoint_key(tenant_id, table_name)
        checkpoint_path = self._checkpoint_path(checkpoint_key)
        if checkpoint_path.exists():
            checkpoint_path.unlink()
        logger.info("Watermark reset", table_name=table_name, tenant_id=str(tenant_id))

    def _extract_delta(
        self,
        source_df: pd.DataFrame,
        watermark_col: str,
        current_watermark: Any,
        cdc_mode: str,
    ) -> pd.DataFrame:
        """Filter source rows to those beyond the current watermark.

        Args:
            source_df: Full source dataset.
            watermark_col: Column used for CDC.
            current_watermark: Last known high watermark value.
            cdc_mode: 'timestamp' | 'sequence' | 'soft_delete'

        Returns:
            DataFrame containing only new/changed rows.
        """
        if watermark_col not in source_df.columns:
            logger.warning(
                "Watermark column not found — treating all rows as delta",
                column=watermark_col,
            )
            return source_df

        if cdc_mode in ("timestamp", "soft_delete"):
            source_col = pd.to_datetime(source_df[watermark_col], errors="coerce", utc=True)
            if isinstance(current_watermark, str):
                try:
                    watermark_dt = pd.Timestamp(current_watermark, tz="UTC")
                except Exception:
                    watermark_dt = pd.Timestamp(_EPOCH, tz="UTC")
            else:
                watermark_dt = pd.Timestamp(_EPOCH, tz="UTC")

            mask = source_col > watermark_dt
        else:
            # Sequence mode: integer comparison
            try:
                wm_int = int(current_watermark) if current_watermark != _EPOCH else 0
            except (ValueError, TypeError):
                wm_int = 0
            source_col_int = pd.to_numeric(source_df[watermark_col], errors="coerce").fillna(0)
            mask = source_col_int > wm_int

        return source_df[mask].copy()

    def _apply_upsert(
        self,
        staging_df: pd.DataFrame,
        delta_batch: pd.DataFrame,
        primary_key: list[str],
        soft_delete_col: str,
    ) -> tuple[pd.DataFrame, int, int, int]:
        """Apply an upsert (insert + update) of delta rows onto the staging dataset.

        Args:
            staging_df: Current staging DataFrame.
            delta_batch: Batch of new/changed rows from the source.
            primary_key: Column(s) identifying unique rows.
            soft_delete_col: Column indicating deleted rows (empty string to disable).

        Returns:
            Tuple of (updated_staging_df, inserted_count, updated_count, deleted_count).
        """
        if staging_df.empty:
            # First load — everything is an insert
            inserted = len(delta_batch)
            if soft_delete_col and soft_delete_col in delta_batch.columns:
                delta_batch = delta_batch[~delta_batch[soft_delete_col].astype(bool)]
                inserted = len(delta_batch)
            return delta_batch.reset_index(drop=True), inserted, 0, 0

        # Validate primary key columns exist in both frames
        valid_pk = [k for k in primary_key if k in staging_df.columns and k in delta_batch.columns]
        if not valid_pk:
            logger.warning(
                "Primary key columns not found — appending delta without dedup",
                attempted_keys=primary_key,
            )
            combined = pd.concat([staging_df, delta_batch], ignore_index=True)
            return combined, len(delta_batch), 0, 0

        # Set index to primary key for efficient lookup
        staging_indexed = staging_df.set_index(valid_pk)
        delta_indexed = delta_batch.set_index(valid_pk)

        deleted_count = 0
        if soft_delete_col and soft_delete_col in delta_indexed.columns:
            deleted_mask = delta_indexed[soft_delete_col].astype(bool)
            deleted_keys = delta_indexed[deleted_mask].index
            staging_indexed = staging_indexed.drop(
                index=staging_indexed.index.intersection(deleted_keys), errors="ignore"
            )
            deleted_count = int(deleted_mask.sum())
            delta_indexed = delta_indexed[~deleted_mask]

        # Identify inserts vs updates
        new_keys = delta_indexed.index.difference(staging_indexed.index)
        existing_keys = delta_indexed.index.intersection(staging_indexed.index)

        inserted_count = len(new_keys)
        updated_count = len(existing_keys)

        # Apply updates
        if not staging_indexed.index.empty and not delta_indexed.loc[existing_keys].empty:
            staging_indexed.update(delta_indexed.loc[existing_keys])

        # Append inserts
        inserts = delta_indexed.loc[new_keys]
        result_indexed = pd.concat([staging_indexed, inserts])

        return result_indexed.reset_index(), inserted_count, updated_count, deleted_count

    def _compute_new_watermark(
        self,
        delta_df: pd.DataFrame,
        watermark_col: str,
        cdc_mode: str,
    ) -> Any:
        """Determine the new high watermark from the processed delta.

        Args:
            delta_df: The delta rows that were just processed.
            watermark_col: Column used for CDC.
            cdc_mode: 'timestamp' | 'sequence' | 'soft_delete'

        Returns:
            The new watermark value (datetime ISO string or int).
        """
        if watermark_col not in delta_df.columns or len(delta_df) == 0:
            return _EPOCH

        if cdc_mode in ("timestamp", "soft_delete"):
            max_ts = pd.to_datetime(delta_df[watermark_col], errors="coerce", utc=True).max()
            if pd.isna(max_ts):
                return _EPOCH
            return max_ts.isoformat()
        else:
            max_seq = pd.to_numeric(delta_df[watermark_col], errors="coerce").max()
            return int(max_seq) if not pd.isna(max_seq) else 0

    def _split_into_batches(
        self,
        df: pd.DataFrame,
        batch_size: int,
    ) -> list[pd.DataFrame]:
        """Split a DataFrame into fixed-size batches.

        Args:
            df: Input DataFrame.
            batch_size: Maximum rows per batch.

        Returns:
            List of DataFrame slices.
        """
        if batch_size <= 0:
            return [df]
        return [df.iloc[i : i + batch_size] for i in range(0, len(df), batch_size)]

    def _build_checkpoint_key(self, tenant_id: uuid.UUID, table_name: str) -> str:
        """Construct a unique checkpoint key for a (tenant, table) pair.

        Args:
            tenant_id: Tenant UUID.
            table_name: Source table logical name.

        Returns:
            Filesystem-safe checkpoint key string.
        """
        safe_table = table_name.replace("/", "_").replace("\\", "_")
        return f"{tenant_id}__{safe_table}"

    def _checkpoint_path(self, checkpoint_key: str) -> Path:
        """Resolve the full path for a checkpoint file.

        Args:
            checkpoint_key: Checkpoint identifier.

        Returns:
            Path object for the checkpoint JSON file.
        """
        return self._checkpoint_dir / f"{checkpoint_key}.json"

    def _load_watermark(self, checkpoint_key: str) -> Any:
        """Read the stored watermark for a checkpoint key.

        Args:
            checkpoint_key: Checkpoint identifier.

        Returns:
            Stored watermark value, or the epoch sentinel if not yet set.
        """
        path = self._checkpoint_path(checkpoint_key)
        if not path.exists():
            return _EPOCH
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data.get("watermark", _EPOCH)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to read checkpoint — resetting watermark", error=str(exc))
            return _EPOCH

    def _save_watermark(self, checkpoint_key: str, watermark: Any) -> None:
        """Persist the new watermark for a checkpoint key.

        Args:
            checkpoint_key: Checkpoint identifier.
            watermark: New watermark value to persist.
        """
        path = self._checkpoint_path(checkpoint_key)
        payload = {
            "watermark": str(watermark),
            "saved_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        }
        try:
            path.write_text(json.dumps(payload), encoding="utf-8")
        except OSError as exc:
            logger.error("Failed to persist checkpoint", checkpoint_key=checkpoint_key, error=str(exc))
