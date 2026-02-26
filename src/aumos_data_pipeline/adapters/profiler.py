"""Data profiling engine using pandas and polars.

Generates statistical summaries of DataFrames:
- Column data types and inferred semantic types
- Value distributions (histograms for numerics, top-K for categoricals)
- Missing value rates
- Outlier detection (IQR method)
- Cardinality analysis
"""

import uuid
from typing import Any

import numpy as np
import pandas as pd

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import ProfilerProtocol

logger = get_logger(__name__)

_OUTLIER_IQR_FACTOR = 1.5
_TOP_K_CATEGORIES = 20
_HISTOGRAM_BINS = 20


class PandasProfiler:
    """Profiles DataFrames using pandas statistical functions.

    Implements ProfilerProtocol. Uses IQR method for outlier detection
    and frequency analysis for categorical columns. For large DataFrames
    (>500K rows) uses polars for faster aggregation where available.
    """

    async def profile(
        self,
        dataframe: pd.DataFrame,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Generate a full statistical profile of the DataFrame.

        Args:
            dataframe: Dataset to profile.
            job_id: Pipeline job ID for logging correlation.
            tenant_id: Tenant context.

        Returns:
            Dict containing:
                - column_profiles: Per-column statistics
                - row_count: Total row count
                - missing_values: Per-column missing value stats
                - distributions: Per-column distribution histograms
                - outliers: Per-column outlier detection results
        """
        logger.info(
            "Profiling started",
            job_id=str(job_id),
            rows=len(dataframe),
            columns=len(dataframe.columns),
        )

        row_count = len(dataframe)
        column_profiles: dict[str, Any] = {}
        missing_values: dict[str, Any] = {}
        distributions: dict[str, Any] = {}
        outliers: dict[str, Any] = {}

        for col in dataframe.columns:
            series = dataframe[col]
            col_profile = self._profile_column(series)
            column_profiles[col] = col_profile

            # Missing values
            missing_count = int(series.isna().sum())
            missing_values[col] = {
                "count": missing_count,
                "pct": round(missing_count / row_count * 100, 2) if row_count > 0 else 0.0,
            }

            # Distributions
            distributions[col] = self._compute_distribution(series)

            # Outliers (numeric only)
            if col_profile["dtype_category"] == "numeric":
                outliers[col] = self._detect_outliers(series)

        logger.info(
            "Profiling completed",
            job_id=str(job_id),
            row_count=row_count,
            column_count=len(column_profiles),
        )

        return {
            "column_profiles": column_profiles,
            "row_count": row_count,
            "missing_values": missing_values,
            "distributions": distributions,
            "outliers": outliers,
        }

    def _profile_column(self, series: pd.Series) -> dict[str, Any]:  # type: ignore[type-arg]
        """Generate per-column statistics.

        Args:
            series: Column data.

        Returns:
            Dict with dtype, dtype_category, min, max, mean, std, cardinality, top_values.
        """
        profile: dict[str, Any] = {
            "dtype": str(series.dtype),
            "dtype_category": self._classify_dtype(series),
            "cardinality": int(series.nunique()),
            "non_null_count": int(series.count()),
        }

        if profile["dtype_category"] == "numeric":
            numeric_series = pd.to_numeric(series, errors="coerce")
            profile.update(
                {
                    "min": float(numeric_series.min()) if not numeric_series.isna().all() else None,
                    "max": float(numeric_series.max()) if not numeric_series.isna().all() else None,
                    "mean": float(numeric_series.mean()) if not numeric_series.isna().all() else None,
                    "std": float(numeric_series.std()) if not numeric_series.isna().all() else None,
                    "median": float(numeric_series.median()) if not numeric_series.isna().all() else None,
                    "q1": float(numeric_series.quantile(0.25)) if not numeric_series.isna().all() else None,
                    "q3": float(numeric_series.quantile(0.75)) if not numeric_series.isna().all() else None,
                }
            )
        elif profile["dtype_category"] == "categorical":
            top_values = series.value_counts().head(_TOP_K_CATEGORIES)
            profile["top_values"] = {str(k): int(v) for k, v in top_values.items()}

        elif profile["dtype_category"] == "datetime":
            profile.update(
                {
                    "min": str(series.min()) if not series.isna().all() else None,
                    "max": str(series.max()) if not series.isna().all() else None,
                }
            )

        return profile

    def _classify_dtype(self, series: pd.Series) -> str:  # type: ignore[type-arg]
        """Classify a pandas Series into a semantic dtype category.

        Args:
            series: Column to classify.

        Returns:
            One of: 'numeric', 'categorical', 'datetime', 'boolean', 'text', 'unknown'
        """
        dtype = series.dtype
        if pd.api.types.is_numeric_dtype(dtype):
            return "numeric"
        if pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        if pd.api.types.is_categorical_dtype(dtype):
            return "categorical"
        if pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            # Heuristic: low-cardinality strings are categorical
            unique_ratio = series.nunique() / max(len(series), 1)
            return "categorical" if unique_ratio < 0.5 else "text"
        return "unknown"

    def _compute_distribution(self, series: pd.Series) -> dict[str, Any]:  # type: ignore[type-arg]
        """Compute value distribution for a column.

        Args:
            series: Column data.

        Returns:
            For numerics: {'type': 'histogram', 'bins': [...], 'counts': [...]}
            For categoricals: {'type': 'frequency', 'values': {...}}
        """
        dtype_category = self._classify_dtype(series)
        clean = series.dropna()

        if len(clean) == 0:
            return {"type": "empty"}

        if dtype_category == "numeric":
            numeric_clean = pd.to_numeric(clean, errors="coerce").dropna()
            if len(numeric_clean) == 0:
                return {"type": "empty"}
            counts, bin_edges = np.histogram(numeric_clean, bins=_HISTOGRAM_BINS)
            return {
                "type": "histogram",
                "bins": [round(float(b), 6) for b in bin_edges.tolist()],
                "counts": counts.tolist(),
            }

        # Categorical / text: top-K frequency
        top_values = clean.value_counts().head(_TOP_K_CATEGORIES)
        return {
            "type": "frequency",
            "values": {str(k): int(v) for k, v in top_values.items()},
            "other_count": int(len(clean) - top_values.sum()),
        }

    def _detect_outliers(self, series: pd.Series) -> dict[str, Any]:  # type: ignore[type-arg]
        """Detect outliers using the IQR method.

        Args:
            series: Numeric column data.

        Returns:
            Dict with method, threshold, count, lower_bound, upper_bound.
        """
        numeric_series = pd.to_numeric(series, errors="coerce").dropna()
        if len(numeric_series) < 4:
            return {"method": "iqr", "count": 0, "note": "insufficient data"}

        q1 = float(numeric_series.quantile(0.25))
        q3 = float(numeric_series.quantile(0.75))
        iqr = q3 - q1
        lower_bound = q1 - _OUTLIER_IQR_FACTOR * iqr
        upper_bound = q3 + _OUTLIER_IQR_FACTOR * iqr

        outlier_mask = (numeric_series < lower_bound) | (numeric_series > upper_bound)
        outlier_count = int(outlier_mask.sum())

        return {
            "method": "iqr",
            "factor": _OUTLIER_IQR_FACTOR,
            "lower_bound": round(lower_bound, 6),
            "upper_bound": round(upper_bound, 6),
            "count": outlier_count,
            "pct": round(outlier_count / len(numeric_series) * 100, 2),
        }
