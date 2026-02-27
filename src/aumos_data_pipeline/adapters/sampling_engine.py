"""Data sampling strategies for the pipeline.

Provides six sampling algorithms for dataset size reduction, train/test splits,
and streaming ingestion:
- Simple random sampling (with reproducible seed)
- Stratified sampling preserving class distribution
- Systematic sampling (every nth row)
- Importance/weighted sampling using a weight column or function
- Reservoir sampling for fixed-size samples from streams
- Sample size calculation via statistical power analysis
"""

import math
import uuid
from typing import Any, Callable

import numpy as np
import pandas as pd

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import SamplingEngineProtocol

logger = get_logger(__name__)

# Minimum rows required before sampling is meaningful
_MIN_ROWS_FOR_SAMPLING = 10
# Default confidence level for sample size calculation
_DEFAULT_CONFIDENCE = 0.95
_Z_SCORES = {0.90: 1.645, 0.95: 1.960, 0.99: 2.576}


class SamplingEngine:
    """Implements configurable sampling strategies for DataFrames.

    Implements SamplingEngineProtocol. All methods are async and return a
    new DataFrame along with sampling metadata (bias report, achieved proportions).

    Sampling config keys (passed to ``sample``):
        - ``strategy``: One of 'random', 'stratified', 'systematic', 'importance',
          'reservoir'
        - ``sample_size``: Target row count (int) OR fraction (float 0–1)
        - ``seed``: Random seed for reproducibility
        - ``stratify_column``: Column to stratify on (stratified strategy)
        - ``weight_column``: Column with importance weights (importance strategy)
        - ``step``: Keep every nth row (systematic strategy)
    """

    async def sample(
        self,
        dataframe: pd.DataFrame,
        sampling_config: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Sample a DataFrame using the configured strategy.

        Args:
            dataframe: Input dataset to sample from.
            sampling_config: Sampling parameters (strategy, sample_size, seed, etc.)
            job_id: Pipeline job ID for audit logging.
            tenant_id: Tenant context.

        Returns:
            Dict containing:
                - dataframe: Sampled DataFrame
                - strategy: Strategy used
                - original_rows: Input row count
                - sampled_rows: Output row count
                - sampling_ratio: Achieved sampling ratio
                - bias_report: Bias detection results per column
        """
        strategy: str = sampling_config.get("strategy", "random")
        seed: int | None = sampling_config.get("seed", None)
        original_rows = len(dataframe)

        logger.info(
            "Sampling started",
            strategy=strategy,
            original_rows=original_rows,
            job_id=str(job_id),
        )

        if original_rows < _MIN_ROWS_FOR_SAMPLING:
            logger.warning(
                "Dataset too small for sampling — returning full dataset",
                rows=original_rows,
                job_id=str(job_id),
            )
            return self._build_result(dataframe, dataframe, strategy, original_rows)

        target_size = self._resolve_target_size(sampling_config, original_rows)

        if strategy == "random":
            sampled = self._random_sample(dataframe, target_size, seed)
        elif strategy == "stratified":
            stratify_col: str = sampling_config.get("stratify_column", "")
            sampled = self._stratified_sample(dataframe, target_size, stratify_col, seed)
        elif strategy == "systematic":
            step: int = sampling_config.get("step", max(1, original_rows // target_size))
            sampled = self._systematic_sample(dataframe, step)
        elif strategy == "importance":
            weight_col: str = sampling_config.get("weight_column", "")
            weight_func: Callable[[pd.DataFrame], pd.Series] | None = sampling_config.get(
                "weight_function", None
            )
            sampled = self._importance_sample(dataframe, target_size, weight_col, weight_func, seed)
        elif strategy == "reservoir":
            sampled = self._reservoir_sample(dataframe, target_size, seed)
        else:
            raise ValueError(f"Unknown sampling strategy: '{strategy}'")

        bias_report = self._detect_bias(original_df=dataframe, sampled_df=sampled)

        logger.info(
            "Sampling completed",
            strategy=strategy,
            original_rows=original_rows,
            sampled_rows=len(sampled),
            job_id=str(job_id),
        )

        return self._build_result(sampled, dataframe, strategy, original_rows, bias_report)

    async def calculate_sample_size(
        self,
        population_size: int,
        margin_of_error: float = 0.05,
        confidence_level: float = _DEFAULT_CONFIDENCE,
        proportion: float = 0.5,
    ) -> dict[str, Any]:
        """Compute the minimum sample size for the given statistical parameters.

        Uses the standard formula for finite population correction:
            n = (Z^2 * p * (1-p) / e^2) / (1 + (Z^2 * p * (1-p)) / (e^2 * N))

        Args:
            population_size: Total population N.
            margin_of_error: Desired margin of error e (default 0.05 = 5%).
            confidence_level: Confidence level (0.90, 0.95, or 0.99).
            proportion: Expected proportion p for worst-case variance (default 0.5).

        Returns:
            Dict with: sample_size, confidence_level, margin_of_error, sampling_ratio.
        """
        z_score = _Z_SCORES.get(confidence_level, 1.960)
        numerator = (z_score ** 2) * proportion * (1.0 - proportion)
        denominator = margin_of_error ** 2
        infinite_population_n = numerator / denominator

        # Finite population correction
        finite_n = infinite_population_n / (
            1.0 + (infinite_population_n - 1.0) / population_size
        )
        sample_size = math.ceil(finite_n)
        sample_size = min(sample_size, population_size)

        logger.info(
            "Sample size calculated",
            population_size=population_size,
            sample_size=sample_size,
            confidence_level=confidence_level,
            margin_of_error=margin_of_error,
        )

        return {
            "sample_size": sample_size,
            "confidence_level": confidence_level,
            "margin_of_error": margin_of_error,
            "proportion": proportion,
            "sampling_ratio": round(sample_size / max(population_size, 1), 4),
        }

    def _random_sample(
        self,
        df: pd.DataFrame,
        target_size: int,
        seed: int | None,
    ) -> pd.DataFrame:
        """Simple random sampling without replacement.

        Args:
            df: Input DataFrame.
            target_size: Target number of rows.
            seed: Random seed for reproducibility.

        Returns:
            Sampled DataFrame.
        """
        rng = np.random.default_rng(seed)
        n = min(target_size, len(df))
        indices = rng.choice(len(df), size=n, replace=False)
        return df.iloc[sorted(indices)].reset_index(drop=True)

    def _stratified_sample(
        self,
        df: pd.DataFrame,
        target_size: int,
        stratify_column: str,
        seed: int | None,
    ) -> pd.DataFrame:
        """Stratified sampling preserving the class distribution.

        Args:
            df: Input DataFrame.
            target_size: Total target sample size.
            stratify_column: Column whose value distribution to preserve.
            seed: Random seed.

        Returns:
            Stratified sample DataFrame.
        """
        if stratify_column not in df.columns:
            logger.warning(
                "Stratify column not found — falling back to random sampling",
                column=stratify_column,
            )
            return self._random_sample(df, target_size, seed)

        rng = np.random.default_rng(seed)
        strata_counts = df[stratify_column].value_counts()
        total_rows = len(df)
        sample_frames: list[pd.DataFrame] = []

        for stratum_value, stratum_count in strata_counts.items():
            stratum_df = df[df[stratify_column] == stratum_value]
            # Proportional allocation
            stratum_target = max(1, round(target_size * stratum_count / total_rows))
            stratum_target = min(stratum_target, len(stratum_df))
            indices = rng.choice(len(stratum_df), size=stratum_target, replace=False)
            sample_frames.append(stratum_df.iloc[sorted(indices)])

        if not sample_frames:
            return df.head(target_size)

        return pd.concat(sample_frames, ignore_index=True)

    def _systematic_sample(
        self,
        df: pd.DataFrame,
        step: int,
    ) -> pd.DataFrame:
        """Systematic sampling: keep every nth row.

        Args:
            df: Input DataFrame.
            step: Select one row every ``step`` rows.

        Returns:
            Systematically sampled DataFrame.
        """
        step = max(1, step)
        return df.iloc[::step].reset_index(drop=True)

    def _importance_sample(
        self,
        df: pd.DataFrame,
        target_size: int,
        weight_column: str,
        weight_function: Callable[[pd.DataFrame], pd.Series] | None,  # type: ignore[type-arg]
        seed: int | None,
    ) -> pd.DataFrame:
        """Importance sampling — rows with higher weights are more likely selected.

        Args:
            df: Input DataFrame.
            target_size: Target row count.
            weight_column: Column name holding importance weights (overrides weight_function).
            weight_function: Callable that receives the DataFrame and returns a weight Series.
            seed: Random seed.

        Returns:
            Importance-weighted sample DataFrame.
        """
        if weight_column and weight_column in df.columns:
            weights = pd.to_numeric(df[weight_column], errors="coerce").fillna(0.0)
        elif weight_function is not None:
            weights = weight_function(df)
        else:
            logger.warning(
                "No weight source for importance sampling — falling back to random",
            )
            return self._random_sample(df, target_size, seed)

        # Normalize weights to probabilities
        total_weight = weights.sum()
        if total_weight <= 0:
            return self._random_sample(df, target_size, seed)

        probabilities = (weights / total_weight).values
        rng = np.random.default_rng(seed)
        n = min(target_size, len(df))
        indices = rng.choice(len(df), size=n, replace=False, p=probabilities)
        return df.iloc[sorted(indices)].reset_index(drop=True)

    def _reservoir_sample(
        self,
        df: pd.DataFrame,
        target_size: int,
        seed: int | None,
    ) -> pd.DataFrame:
        """Reservoir sampling (Algorithm R) for streaming-compatible fixed-size samples.

        Processes the DataFrame as if it were a stream — guarantees uniform
        probability without knowing the population size upfront.

        Args:
            df: Input DataFrame (treated as a stream of rows).
            target_size: Fixed reservoir size k.
            seed: Random seed.

        Returns:
            Reservoir sample of exactly min(k, n) rows.
        """
        rng = np.random.default_rng(seed)
        n = len(df)
        k = min(target_size, n)

        # Initialize reservoir with first k items
        reservoir_indices = list(range(k))

        # For each subsequent item, randomly replace a reservoir entry
        for i in range(k, n):
            j = int(rng.integers(0, i + 1))
            if j < k:
                reservoir_indices[j] = i

        selected = sorted(reservoir_indices)
        return df.iloc[selected].reset_index(drop=True)

    def _detect_bias(
        self,
        original_df: pd.DataFrame,
        sampled_df: pd.DataFrame,
    ) -> dict[str, Any]:
        """Compare column distributions between original and sampled datasets.

        Measures sampling bias as the divergence between original and sampled
        value frequencies for categorical columns, and mean/std drift for numerics.

        Args:
            original_df: Full original dataset.
            sampled_df: Sampled subset.

        Returns:
            Dict with bias metrics per column: {col: {bias_type, drift_score, verdict}}
        """
        bias_report: dict[str, Any] = {}

        for col in original_df.columns:
            if col not in sampled_df.columns:
                continue

            col_bias: dict[str, Any] = {}

            if pd.api.types.is_numeric_dtype(original_df[col]):
                orig_mean = float(original_df[col].mean())
                samp_mean = float(sampled_df[col].mean())
                orig_std = float(original_df[col].std()) or 1.0
                normalized_drift = abs(orig_mean - samp_mean) / orig_std
                col_bias = {
                    "bias_type": "numeric_mean_drift",
                    "original_mean": round(orig_mean, 6),
                    "sampled_mean": round(samp_mean, 6),
                    "drift_score": round(normalized_drift, 6),
                    "verdict": "biased" if normalized_drift > 0.1 else "unbiased",
                }
            else:
                orig_dist = original_df[col].value_counts(normalize=True)
                samp_dist = sampled_df[col].value_counts(normalize=True)
                all_values = orig_dist.index.union(samp_dist.index)
                tvd = float(
                    sum(
                        abs(orig_dist.get(v, 0.0) - samp_dist.get(v, 0.0))
                        for v in all_values
                    )
                    / 2.0
                )
                col_bias = {
                    "bias_type": "categorical_tvd",
                    "total_variation_distance": round(tvd, 6),
                    "verdict": "biased" if tvd > 0.05 else "unbiased",
                }

            bias_report[col] = col_bias

        return bias_report

    def _resolve_target_size(
        self,
        sampling_config: dict[str, Any],
        original_rows: int,
    ) -> int:
        """Resolve 'sample_size' from the config to an absolute row count.

        Args:
            sampling_config: Sampling configuration dict.
            original_rows: Total rows in the input DataFrame.

        Returns:
            Target number of rows as a positive integer.
        """
        sample_size = sampling_config.get("sample_size", 0.1)
        if isinstance(sample_size, float) and 0.0 < sample_size <= 1.0:
            return max(1, round(original_rows * sample_size))
        return max(1, int(sample_size))

    def _build_result(
        self,
        sampled: pd.DataFrame,
        original: pd.DataFrame,
        strategy: str,
        original_rows: int,
        bias_report: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the standard sampling result dict.

        Args:
            sampled: The sampled DataFrame.
            original: The original DataFrame.
            strategy: Name of the strategy used.
            original_rows: Row count of the original dataset.
            bias_report: Optional bias detection results.

        Returns:
            Standardized result dict.
        """
        sampled_rows = len(sampled)
        return {
            "dataframe": sampled,
            "strategy": strategy,
            "original_rows": original_rows,
            "sampled_rows": sampled_rows,
            "sampling_ratio": round(sampled_rows / max(original_rows, 1), 4),
            "bias_report": bias_report or {},
        }
