"""Great Expectations quality gate adapter.

Validates DataFrames against expectation suites stored in MinIO.
Computes a quality score (passed_expectations / total_expectations)
and returns structured validation results.
"""

import uuid
from typing import Any

import pandas as pd

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import QualityGateProtocol

logger = get_logger(__name__)


class GreatExpectationsGate:
    """Validates datasets using Great Expectations expectation suites.

    Implements QualityGateProtocol. Loads expectation suites from MinIO
    (at s3://aumos-data/ge-suites/{tenant_id}/{suite_name}.json) and
    runs them against the provided DataFrame.

    Falls back to a default suite of basic sanity checks if no
    custom suite is found for the tenant.
    """

    def __init__(self, storage: Any) -> None:
        """Initialize with storage adapter for loading expectation suites.

        Args:
            storage: Storage adapter (MinioStorage) for reading suite files.
        """
        self._storage = storage

    async def validate(
        self,
        dataframe: pd.DataFrame,
        expectation_suite: str,
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Validate a DataFrame against a Great Expectations suite.

        Args:
            dataframe: Dataset to validate.
            expectation_suite: Name of the expectation suite.
            job_id: Pipeline job ID for correlation.
            tenant_id: Tenant context (suites are namespaced per tenant).

        Returns:
            Dict with:
                - passed: bool — whether quality_score >= threshold
                - quality_score: float — passed_expectations / total_expectations
                - results: dict — full GE validation result JSON
        """
        logger.info(
            "Quality gate validation starting",
            suite=expectation_suite,
            job_id=str(job_id),
            rows=len(dataframe),
        )

        try:
            import great_expectations as ge  # noqa: PLC0415

            # Load expectation suite from storage or use default
            suite_data = await self._load_suite(expectation_suite, tenant_id)

            # Create GE dataset
            ge_df = ge.from_pandas(dataframe)

            # Apply expectations from suite
            results_list: list[dict[str, Any]] = []
            expectations = suite_data.get("expectations", self._default_expectations(dataframe))

            for expectation in expectations:
                exp_type = expectation.get("expectation_type", "")
                kwargs = expectation.get("kwargs", {})

                result = self._apply_expectation(ge_df, exp_type, kwargs)
                results_list.append(result)

            # Compute quality score
            total = len(results_list)
            passed_count = sum(1 for r in results_list if r.get("success", False))
            quality_score = passed_count / total if total > 0 else 0.0

            results = {
                "expectation_suite_name": expectation_suite,
                "results": results_list,
                "statistics": {
                    "evaluated_expectations": total,
                    "successful_expectations": passed_count,
                    "unsuccessful_expectations": total - passed_count,
                    "success_percent": round(quality_score * 100, 2),
                },
            }

            logger.info(
                "Quality gate validation completed",
                suite=expectation_suite,
                job_id=str(job_id),
                quality_score=quality_score,
                passed=quality_score >= 0.8,
            )

            return {
                "passed": quality_score >= 0.8,
                "quality_score": round(quality_score, 4),
                "results": results,
            }

        except ImportError:
            logger.warning("great-expectations not available — using fallback validation")
            return await self._fallback_validate(dataframe, expectation_suite, job_id)
        except Exception as exc:
            logger.error("Quality gate validation failed", error=str(exc), job_id=str(job_id))
            return {
                "passed": False,
                "quality_score": 0.0,
                "results": {"error": str(exc)},
            }

    async def _load_suite(self, suite_name: str, tenant_id: uuid.UUID) -> dict[str, Any]:
        """Load an expectation suite from MinIO.

        Args:
            suite_name: Suite name.
            tenant_id: Tenant context for namespacing.

        Returns:
            Expectation suite dict, or empty dict if not found.
        """
        try:
            suite_uri = f"s3://aumos-data/ge-suites/{tenant_id}/{suite_name}.json"
            return await self._storage.read_json(suite_uri) or {}
        except Exception:
            return {}

    def _default_expectations(self, dataframe: pd.DataFrame) -> list[dict[str, Any]]:
        """Generate a default set of basic sanity check expectations.

        Args:
            dataframe: DataFrame to generate expectations for.

        Returns:
            List of expectation dicts.
        """
        expectations: list[dict[str, Any]] = [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": None},
            }
        ]
        for col in dataframe.columns:
            expectations.append(
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": col},
                }
            )
        return expectations

    def _apply_expectation(
        self,
        ge_df: Any,
        expectation_type: str,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """Apply a single expectation to the GE DataFrame.

        Args:
            ge_df: Great Expectations DataFrame.
            expectation_type: Name of the GE expectation method.
            kwargs: Expectation keyword arguments.

        Returns:
            Result dict with 'success' key.
        """
        try:
            method = getattr(ge_df, expectation_type, None)
            if method is None:
                return {"success": False, "error": f"Unknown expectation: {expectation_type}"}
            result = method(**kwargs)
            return {"success": result.success, "expectation_type": expectation_type, "result": result.to_json_dict()}
        except Exception as exc:
            return {"success": False, "expectation_type": expectation_type, "error": str(exc)}

    async def _fallback_validate(
        self,
        dataframe: pd.DataFrame,
        expectation_suite: str,
        job_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Perform basic validation when GE is unavailable.

        Args:
            dataframe: Dataset to validate.
            expectation_suite: Suite name (for logging).
            job_id: Job ID for correlation.

        Returns:
            Validation result with basic checks.
        """
        checks: list[dict[str, Any]] = []

        # Check 1: Non-empty
        checks.append({
            "check": "non_empty",
            "success": len(dataframe) > 0,
            "value": len(dataframe),
        })

        # Check 2: No fully-null columns
        for col in dataframe.columns:
            null_pct = dataframe[col].isna().sum() / max(len(dataframe), 1)
            checks.append({
                "check": f"column_{col}_not_fully_null",
                "success": null_pct < 1.0,
                "null_pct": round(float(null_pct), 4),
            })

        passed_count = sum(1 for c in checks if c["success"])
        quality_score = passed_count / len(checks) if checks else 0.0

        return {
            "passed": quality_score >= 0.8,
            "quality_score": round(quality_score, 4),
            "results": {
                "expectation_suite_name": expectation_suite,
                "fallback_mode": True,
                "checks": checks,
            },
        }
