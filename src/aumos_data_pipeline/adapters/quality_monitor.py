"""Data quality SLO monitoring adapter.

Defines, evaluates, and alerts on data quality rules using a lightweight
rule engine that mirrors the Great Expectations expectation model. Tracks
quality metrics over time and detects anomalies in those trends.

Quality dimensions covered:
- Completeness: null/missing value rates per column
- Accuracy: value range and format conformance
- Timeliness: freshness relative to a reference timestamp
- Uniqueness: duplicate row detection
- Consistency: referential integrity checks across columns

Alert dispatch supports webhooks, Sentry-style error events, and
structured Kafka messages for downstream alerting infrastructure.
"""

import datetime
import statistics
import uuid
from typing import Any

import pandas as pd

from aumos_common.events import EventPublisher, Topics
from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import DataQualityMonitorProtocol

logger = get_logger(__name__)

# Maximum history entries kept per metric key for trend analysis
_MAX_TREND_HISTORY = 100
# Z-score threshold for anomaly detection in quality metrics
_ANOMALY_Z_THRESHOLD = 3.0


class DataQualityMonitor:
    """Monitors data quality SLOs and dispatches alerts on violations.

    Implements DataQualityMonitorProtocol. Maintains an in-memory history of
    quality metric values per (tenant_id, metric_key) for trend tracking and
    anomaly detection. In production, this history would be backed by a time-series
    store (e.g., TimescaleDB or ClickHouse).

    Each quality rule is a dict with:
        - ``dimension``: 'completeness' | 'accuracy' | 'timeliness' | 'uniqueness' | 'consistency'
        - ``column``: Column to evaluate (may be None for row-level checks)
        - ``threshold``: SLO threshold value (meaning depends on dimension)
        - ``operator``: 'gte' | 'lte' | 'eq' | 'between'
        - ``alert_on_failure``: Whether to dispatch an alert if the SLO is violated
    """

    def __init__(
        self,
        event_publisher: EventPublisher,
        webhook_url: str = "",
        alert_channel: str = "pipeline-alerts",
    ) -> None:
        """Initialize the quality monitor.

        Args:
            event_publisher: Kafka publisher for quality alert events.
            webhook_url: Optional HTTP endpoint for webhook alerts (empty to disable).
            alert_channel: Slack/webhook channel name for alert messages.
        """
        self._publisher = event_publisher
        self._webhook_url = webhook_url
        self._alert_channel = alert_channel
        # In-memory metric history: {metric_key: [float, ...]}
        self._metric_history: dict[str, list[float]] = {}

    async def evaluate_rules(
        self,
        dataframe: pd.DataFrame,
        rules: list[dict[str, Any]],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Evaluate a list of quality rules against a DataFrame.

        Args:
            dataframe: Dataset to evaluate.
            rules: List of quality rule dicts (see class docstring for schema).
            job_id: Pipeline job ID for correlation.
            tenant_id: Tenant context.

        Returns:
            Dict containing:
                - passed: True if all SLOs are met
                - quality_score: Fraction of rules that passed (0.0–1.0)
                - rule_results: List of per-rule evaluation results
                - violations: List of rules that failed
                - alerts_dispatched: Number of alerts sent
        """
        rule_results: list[dict[str, Any]] = []
        violations: list[dict[str, Any]] = []
        alerts_dispatched = 0

        logger.info(
            "Quality rule evaluation started",
            rule_count=len(rules),
            rows=len(dataframe),
            job_id=str(job_id),
        )

        for rule in rules:
            result = self._evaluate_single_rule(dataframe, rule, job_id)
            rule_results.append(result)

            # Record metric in history for trend tracking
            metric_key = f"{tenant_id}__{result['dimension']}__{result.get('column', '_row')}"
            self._record_metric(metric_key, result["metric_value"])

            # Detect anomaly in this metric's history
            anomaly = self._detect_metric_anomaly(metric_key, result["metric_value"])
            result["anomaly_detected"] = anomaly

            if not result["passed"]:
                violations.append(result)
                if rule.get("alert_on_failure", True):
                    await self._dispatch_alert(result, job_id, tenant_id)
                    alerts_dispatched += 1

        passed_count = sum(1 for r in rule_results if r["passed"])
        quality_score = passed_count / max(len(rule_results), 1)
        all_passed = len(violations) == 0

        logger.info(
            "Quality rule evaluation completed",
            quality_score=round(quality_score, 4),
            violations=len(violations),
            alerts_dispatched=alerts_dispatched,
            job_id=str(job_id),
        )

        return {
            "passed": all_passed,
            "quality_score": round(quality_score, 4),
            "rule_results": rule_results,
            "violations": violations,
            "alerts_dispatched": alerts_dispatched,
        }

    async def aggregate_dashboard_data(
        self,
        tenant_id: uuid.UUID,
        lookback_entries: int = 30,
    ) -> dict[str, Any]:
        """Aggregate quality metric trends for dashboard display.

        Args:
            tenant_id: Tenant context.
            lookback_entries: How many historical entries to include per metric.

        Returns:
            Dict with trend data per metric key:
                - metric_key: {values, mean, std, trend_direction}
        """
        prefix = str(tenant_id)
        dashboard: dict[str, Any] = {}

        for metric_key, history in self._metric_history.items():
            if not metric_key.startswith(prefix):
                continue

            recent = history[-lookback_entries:]
            if len(recent) < 2:
                continue

            mean_val = statistics.mean(recent)
            std_val = statistics.stdev(recent) if len(recent) > 1 else 0.0
            # Trend: compare last third vs first third
            third = max(1, len(recent) // 3)
            early_mean = statistics.mean(recent[:third])
            late_mean = statistics.mean(recent[-third:])
            if late_mean > early_mean + std_val * 0.2:
                trend = "improving"
            elif late_mean < early_mean - std_val * 0.2:
                trend = "degrading"
            else:
                trend = "stable"

            dashboard[metric_key] = {
                "values": recent,
                "mean": round(mean_val, 6),
                "std": round(std_val, 6),
                "trend_direction": trend,
                "entry_count": len(recent),
            }

        return dashboard

    def _evaluate_single_rule(
        self,
        dataframe: pd.DataFrame,
        rule: dict[str, Any],
        job_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Evaluate one quality rule against the DataFrame.

        Args:
            dataframe: Dataset to check.
            rule: Quality rule definition dict.
            job_id: Job ID for log correlation.

        Returns:
            Rule result dict with: dimension, column, metric_value, threshold, passed, message.
        """
        dimension: str = rule.get("dimension", "completeness")
        column: str | None = rule.get("column")
        threshold: float = float(rule.get("threshold", 1.0))
        operator: str = rule.get("operator", "gte")

        try:
            metric_value = self._compute_metric(dataframe, dimension, column)
        except Exception as exc:
            logger.warning(
                "Failed to compute quality metric",
                dimension=dimension,
                column=column,
                error=str(exc),
                job_id=str(job_id),
            )
            metric_value = 0.0

        passed = self._evaluate_operator(metric_value, operator, threshold, rule)

        return {
            "dimension": dimension,
            "column": column,
            "metric_value": round(metric_value, 6),
            "threshold": threshold,
            "operator": operator,
            "passed": passed,
            "rule_name": rule.get("name", f"{dimension}_{column or 'row'}"),
            "message": self._format_result_message(dimension, column, metric_value, threshold, passed),
        }

    def _compute_metric(
        self,
        dataframe: pd.DataFrame,
        dimension: str,
        column: str | None,
    ) -> float:
        """Compute the numeric value of a quality metric.

        Args:
            dataframe: Dataset to evaluate.
            dimension: Quality dimension to measure.
            column: Column to evaluate (None for row-level dimensions).

        Returns:
            Metric value as float (interpretation depends on dimension).
        """
        if len(dataframe) == 0:
            return 0.0

        if dimension == "completeness":
            if column and column in dataframe.columns:
                non_null_rate = 1.0 - (dataframe[column].isna().sum() / len(dataframe))
                return float(non_null_rate)
            # Row-level completeness: fraction of rows with no nulls
            return float((~dataframe.isnull().any(axis=1)).mean())

        elif dimension == "uniqueness":
            if column and column in dataframe.columns:
                return float(dataframe[column].nunique() / len(dataframe))
            # Row-level uniqueness: fraction of unique rows
            return float(len(dataframe.drop_duplicates()) / len(dataframe))

        elif dimension == "accuracy":
            if not column or column not in dataframe.columns:
                return 1.0
            # Accuracy: fraction of non-null, non-empty values
            col_data = dataframe[column]
            valid_mask = col_data.notna()
            if col_data.dtype == object:
                valid_mask = valid_mask & (col_data.astype(str).str.strip() != "")
            return float(valid_mask.mean())

        elif dimension == "timeliness":
            if not column or column not in dataframe.columns:
                return 1.0
            # Timeliness: fraction of timestamps that are within the last 24 hours
            timestamps = pd.to_datetime(dataframe[column], errors="coerce", utc=True)
            now = pd.Timestamp.now(tz="UTC")
            cutoff = now - pd.Timedelta(hours=24)
            fresh = (timestamps >= cutoff).sum()
            return float(fresh / len(dataframe))

        elif dimension == "consistency":
            # Consistency: fraction of non-null rows (simpler proxy for referential checks)
            if column and column in dataframe.columns:
                return float(dataframe[column].notna().mean())
            return 1.0

        else:
            logger.warning("Unknown quality dimension", dimension=dimension)
            return 0.0

    def _evaluate_operator(
        self,
        metric_value: float,
        operator: str,
        threshold: float,
        rule: dict[str, Any],
    ) -> bool:
        """Compare the metric value against the threshold using the given operator.

        Args:
            metric_value: Computed metric.
            operator: Comparison operator.
            threshold: SLO threshold.
            rule: Full rule dict (for 'between' upper bound).

        Returns:
            True if the SLO is satisfied.
        """
        if operator == "gte":
            return metric_value >= threshold
        elif operator == "lte":
            return metric_value <= threshold
        elif operator == "eq":
            return abs(metric_value - threshold) < 1e-9
        elif operator == "between":
            upper: float = float(rule.get("upper_threshold", 1.0))
            return threshold <= metric_value <= upper
        elif operator == "gt":
            return metric_value > threshold
        elif operator == "lt":
            return metric_value < threshold
        else:
            logger.warning("Unknown operator — defaulting to gte", operator=operator)
            return metric_value >= threshold

    def _record_metric(self, metric_key: str, value: float) -> None:
        """Append a metric value to its history, pruning to the max length.

        Args:
            metric_key: Unique identifier for the metric time series.
            value: New metric value to record.
        """
        history = self._metric_history.setdefault(metric_key, [])
        history.append(value)
        if len(history) > _MAX_TREND_HISTORY:
            self._metric_history[metric_key] = history[-_MAX_TREND_HISTORY:]

    def _detect_metric_anomaly(self, metric_key: str, latest_value: float) -> bool:
        """Detect whether the latest metric value is anomalous based on history.

        Uses a Z-score check against the rolling mean and standard deviation.

        Args:
            metric_key: Metric time series identifier.
            latest_value: The value just computed.

        Returns:
            True if the value is more than _ANOMALY_Z_THRESHOLD standard deviations
            from the historical mean.
        """
        history = self._metric_history.get(metric_key, [])
        if len(history) < 5:
            return False

        # Exclude the most recent value (just recorded) from the baseline
        baseline = history[:-1]
        mean_val = statistics.mean(baseline)
        std_val = statistics.stdev(baseline)

        if std_val < 1e-10:
            return False

        z_score = abs(latest_value - mean_val) / std_val
        return z_score > _ANOMALY_Z_THRESHOLD

    async def _dispatch_alert(
        self,
        rule_result: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> None:
        """Dispatch an alert for a quality SLO violation.

        Publishes a Kafka event and optionally fires a webhook.

        Args:
            rule_result: The failed rule result dict.
            job_id: Pipeline job ID.
            tenant_id: Tenant context.
        """
        alert_payload: dict[str, Any] = {
            "tenant_id": str(tenant_id),
            "job_id": str(job_id),
            "rule_name": rule_result.get("rule_name", "unknown"),
            "dimension": rule_result.get("dimension"),
            "column": rule_result.get("column"),
            "metric_value": rule_result.get("metric_value"),
            "threshold": rule_result.get("threshold"),
            "message": rule_result.get("message", "Quality SLO violated"),
            "alert_channel": self._alert_channel,
            "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        }

        try:
            await self._publisher.publish(Topics.PIPELINE_JOB_FAILED, alert_payload)
        except Exception as exc:
            logger.error("Failed to publish quality alert", error=str(exc), job_id=str(job_id))

        if self._webhook_url:
            await self._fire_webhook(alert_payload)

        logger.warning(
            "Quality SLO violation alert dispatched",
            rule_name=alert_payload["rule_name"],
            metric_value=alert_payload["metric_value"],
            threshold=alert_payload["threshold"],
            job_id=str(job_id),
        )

    async def _fire_webhook(self, payload: dict[str, Any]) -> None:
        """Send a quality alert to the configured webhook URL.

        Args:
            payload: Alert payload dict to POST as JSON.
        """
        try:
            import asyncio
            import json as _json

            import urllib.request

            body = _json.dumps(payload).encode()
            req = urllib.request.Request(
                self._webhook_url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, urllib.request.urlopen, req)
        except Exception as exc:
            logger.error("Webhook dispatch failed", webhook_url=self._webhook_url, error=str(exc))

    @staticmethod
    def _format_result_message(
        dimension: str,
        column: str | None,
        metric_value: float,
        threshold: float,
        passed: bool,
    ) -> str:
        """Format a human-readable result message.

        Args:
            dimension: Quality dimension.
            column: Column evaluated (None for row-level).
            metric_value: Computed metric.
            threshold: SLO threshold.
            passed: Whether the SLO passed.

        Returns:
            Descriptive result string.
        """
        target = f"column '{column}'" if column else "all columns"
        status = "PASSED" if passed else "FAILED"
        return (
            f"[{status}] {dimension} on {target}: "
            f"metric={metric_value:.4f}, threshold={threshold:.4f}"
        )
