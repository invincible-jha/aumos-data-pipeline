"""Data cleaning pipeline implementation.

Applies a configurable sequence of cleaning operations:
1. Deduplication
2. Missing value imputation
3. Outlier handling
4. Categorical encoding
5. Column normalization
"""

from typing import Any

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, StandardScaler

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import CleanerProtocol

logger = get_logger(__name__)


class DataCleaner:
    """Cleans and normalizes DataFrames.

    Implements CleanerProtocol. Operations are applied in order:
    dedup → impute → outliers → encode → normalize.

    All operations are non-destructive to the original input:
    a new DataFrame is returned.
    """

    async def clean(
        self,
        dataframe: pd.DataFrame,
        cleaning_config: dict[str, Any],
    ) -> pd.DataFrame:
        """Apply the full cleaning pipeline.

        Args:
            dataframe: Raw dataset.
            cleaning_config: Cleaning configuration dict:
                - dedup_subset: List of columns for dedup (empty = all)
                - imputation_strategy: 'mean' | 'median' | 'mode' | 'drop' | 'none'
                - outlier_handling: 'clip' | 'remove' | 'none'
                - encoding: {col_name: 'onehot' | 'label' | 'ordinal'}
                - normalize: {col_name: 'minmax' | 'zscore'}
                - drop_cols: List of columns to drop

        Returns:
            Cleaned DataFrame.
        """
        df = dataframe.copy()
        rows_start = len(df)

        logger.info("Cleaning pipeline started", rows=rows_start, columns=len(df.columns))

        # Step 1: Drop specified columns
        drop_cols: list[str] = cleaning_config.get("drop_cols", [])
        if drop_cols:
            existing_drops = [c for c in drop_cols if c in df.columns]
            df = df.drop(columns=existing_drops)
            logger.info("Dropped columns", columns=existing_drops)

        # Step 2: Deduplication
        dedup_subset: list[str] | None = cleaning_config.get("dedup_subset") or None
        df = self._deduplicate(df, dedup_subset)

        # Step 3: Missing value imputation
        imputation_strategy: str = cleaning_config.get("imputation_strategy", "none")
        if imputation_strategy != "none":
            df = self._impute(df, imputation_strategy)

        # Step 4: Outlier handling
        outlier_handling: str = cleaning_config.get("outlier_handling", "none")
        if outlier_handling != "none":
            df = self._handle_outliers(df, outlier_handling)

        # Step 5: Categorical encoding
        encoding: dict[str, str] = cleaning_config.get("encoding", {})
        if encoding:
            df = self._encode(df, encoding)

        # Step 6: Normalization
        normalize: dict[str, str] = cleaning_config.get("normalize", {})
        if normalize:
            df = self._normalize(df, normalize)

        rows_end = len(df)
        logger.info(
            "Cleaning pipeline completed",
            rows_before=rows_start,
            rows_after=rows_end,
            rows_removed=rows_start - rows_end,
        )
        return df

    def _deduplicate(self, df: pd.DataFrame, subset: list[str] | None) -> pd.DataFrame:
        """Remove duplicate rows.

        Args:
            df: Input DataFrame.
            subset: Columns to use for identifying duplicates (None = all columns).

        Returns:
            DataFrame with duplicates removed.
        """
        before = len(df)
        # Validate subset columns exist
        valid_subset: list[str] | None = None
        if subset:
            valid_subset = [c for c in subset if c in df.columns]

        df = df.drop_duplicates(subset=valid_subset or None, keep="first")
        removed = before - len(df)
        if removed > 0:
            logger.info("Deduplication removed rows", count=removed)
        return df

    def _impute(self, df: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """Impute missing values.

        Args:
            df: Input DataFrame.
            strategy: 'mean' | 'median' | 'mode' | 'drop'

        Returns:
            DataFrame with missing values handled.
        """
        if strategy == "drop":
            return df.dropna()

        for col in df.columns:
            if df[col].isna().sum() == 0:
                continue

            if pd.api.types.is_numeric_dtype(df[col]):
                if strategy == "mean":
                    fill_value = df[col].mean()
                elif strategy == "median":
                    fill_value = df[col].median()
                else:  # mode
                    mode_values = df[col].mode()
                    fill_value = mode_values.iloc[0] if not mode_values.empty else 0
                df[col] = df[col].fillna(fill_value)
            else:
                # Non-numeric: always use mode
                mode_values = df[col].mode()
                if not mode_values.empty:
                    df[col] = df[col].fillna(mode_values.iloc[0])

        return df

    def _handle_outliers(self, df: pd.DataFrame, method: str) -> pd.DataFrame:
        """Handle outliers in numeric columns using IQR bounds.

        Args:
            df: Input DataFrame.
            method: 'clip' (cap at bounds) | 'remove' (drop rows)

        Returns:
            DataFrame with outliers handled.
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            return df

        if method == "clip":
            for col in numeric_cols:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                df[col] = df[col].clip(lower=lower, upper=upper)
        elif method == "remove":
            mask = pd.Series([True] * len(df), index=df.index)
            for col in numeric_cols:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                mask = mask & df[col].between(lower, upper)
            df = df[mask]

        return df

    def _encode(self, df: pd.DataFrame, encoding: dict[str, str]) -> pd.DataFrame:
        """Apply categorical encoding.

        Args:
            df: Input DataFrame.
            encoding: Map of {col_name: 'onehot' | 'label' | 'ordinal'}

        Returns:
            DataFrame with encoded columns.
        """
        for col, enc_type in encoding.items():
            if col not in df.columns:
                logger.warning("Encoding skipped — column not found", column=col)
                continue

            if enc_type == "onehot":
                dummies = pd.get_dummies(df[col], prefix=col, drop_first=False)
                df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
            elif enc_type in ("label", "ordinal"):
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))

        return df

    def _normalize(self, df: pd.DataFrame, normalize: dict[str, str]) -> pd.DataFrame:
        """Normalize numeric columns.

        Args:
            df: Input DataFrame.
            normalize: Map of {col_name: 'minmax' | 'zscore'}

        Returns:
            DataFrame with normalized columns.
        """
        for col, norm_type in normalize.items():
            if col not in df.columns:
                logger.warning("Normalization skipped — column not found", column=col)
                continue

            if not pd.api.types.is_numeric_dtype(df[col]):
                logger.warning("Normalization skipped — non-numeric column", column=col)
                continue

            values = df[[col]].values

            if norm_type == "minmax":
                scaler = MinMaxScaler()
            else:  # zscore
                scaler = StandardScaler()

            df[col] = scaler.fit_transform(values).flatten()

        return df
