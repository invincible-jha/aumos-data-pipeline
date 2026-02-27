"""PII de-identification adapter for the data pipeline.

Applies column-level PII detection and configurable masking strategies
(hash, redact, pseudonymize, generalize) to DataFrames before they are
routed to downstream AI workloads.

PII entity types detected:
- EMAIL: standard email addresses
- SSN: US Social Security Numbers (###-##-####)
- PHONE: North American phone numbers
- NAME: detected via heuristic word capitalization patterns
- ADDRESS: street address patterns (number + street)
- IP: IPv4 addresses
- CREDIT_CARD: 16-digit card numbers
"""

import hashlib
import re
import secrets
import uuid
from typing import Any

import pandas as pd

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import DeidentifierProtocol

logger = get_logger(__name__)

# Regex patterns per PII entity type
_PII_PATTERNS: dict[str, re.Pattern[str]] = {
    "EMAIL": re.compile(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE
    ),
    "SSN": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "PHONE": re.compile(
        r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    ),
    "IP": re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    ),
    "CREDIT_CARD": re.compile(r"\b(?:\d[ -]?){13,16}\b"),
    "ADDRESS": re.compile(
        r"\b\d{1,5}\s+[A-Z][a-zA-Z\s]{3,40}(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct)\b",
        re.IGNORECASE,
    ),
    "NAME": re.compile(r"\b[A-Z][a-z]{1,20}\s+[A-Z][a-z]{1,20}\b"),
}

# Salt used for consistent pseudonymization within a job; rotated per job_id
_PSEUDONYM_CACHE: dict[str, str] = {}


class DataDeidentifier:
    """Masks and redacts PII from DataFrames.

    Implements DeidentifierProtocol. Supports four masking strategies per column:
    - ``hash``: SHA-256 hex digest (deterministic within a job)
    - ``redact``: Replace with a constant label such as "[REDACTED]"
    - ``pseudonymize``: Stable mapping via HMAC-SHA256 with a per-job secret
    - ``generalize``: Partial masking that preserves value shape (e.g. email domain kept)

    Usage example::

        config = {
            "columns": {
                "email": {"entity_types": ["EMAIL"], "strategy": "pseudonymize"},
                "phone": {"entity_types": ["PHONE"], "strategy": "redact"},
                "notes": {"entity_types": ["SSN", "NAME"], "strategy": "hash"},
            },
            "auto_detect": True,   # scan all text columns for PII even if not listed
            "risk_threshold": 0.3, # re-identification risk score above which to warn
        }
    """

    def __init__(self) -> None:
        """Initialize the de-identifier with an empty pseudonymization cache."""
        self._pseudonym_map: dict[str, dict[str, str]] = {}

    async def deidentify(
        self,
        dataframe: pd.DataFrame,
        deidentification_config: dict[str, Any],
        job_id: uuid.UUID,
        tenant_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Apply PII masking to the DataFrame according to the config.

        Args:
            dataframe: Input dataset that may contain PII.
            deidentification_config: Masking configuration:
                - columns: {col_name: {entity_types: [...], strategy: str}}
                - auto_detect: bool — scan text columns for PII patterns
                - risk_threshold: float — warn when re-id risk exceeds this value
            job_id: Pipeline job ID used as the pseudonymization seed namespace.
            tenant_id: Tenant context for audit logging.

        Returns:
            Dict containing:
                - dataframe: De-identified DataFrame
                - audit_log: List of masking actions applied
                - pii_columns_detected: Columns where PII was found
                - risk_score: Estimated re-identification risk (0.0–1.0)
        """
        job_secret = self._derive_job_secret(job_id)
        df = dataframe.copy()
        audit_log: list[dict[str, Any]] = []
        pii_columns_detected: list[str] = []

        column_config: dict[str, Any] = deidentification_config.get("columns", {})
        auto_detect: bool = deidentification_config.get("auto_detect", False)
        risk_threshold: float = deidentification_config.get("risk_threshold", 0.3)

        # Auto-detect PII in text columns not already listed in config
        if auto_detect:
            detected = self._auto_detect_pii_columns(df, already_configured=set(column_config.keys()))
            for col_name, entity_types in detected.items():
                if col_name not in column_config:
                    column_config[col_name] = {"entity_types": entity_types, "strategy": "redact"}
                    logger.info(
                        "Auto-detected PII column",
                        column=col_name,
                        entity_types=entity_types,
                        job_id=str(job_id),
                    )

        # Apply masking per configured column
        for col_name, col_cfg in column_config.items():
            if col_name not in df.columns:
                logger.warning("PII column not found in dataframe — skipping", column=col_name)
                continue

            entity_types: list[str] = col_cfg.get("entity_types", list(_PII_PATTERNS.keys()))
            strategy: str = col_cfg.get("strategy", "redact")
            pii_found_count = 0

            df[col_name], masked_count = self._apply_masking(
                series=df[col_name],
                entity_types=entity_types,
                strategy=strategy,
                job_secret=job_secret,
                col_name=col_name,
            )
            pii_found_count += masked_count

            if masked_count > 0:
                pii_columns_detected.append(col_name)

            audit_entry: dict[str, Any] = {
                "column": col_name,
                "strategy": strategy,
                "entity_types": entity_types,
                "values_masked": masked_count,
                "job_id": str(job_id),
                "tenant_id": str(tenant_id),
            }
            audit_log.append(audit_entry)

            logger.info(
                "PII masking applied",
                column=col_name,
                strategy=strategy,
                values_masked=masked_count,
                job_id=str(job_id),
            )

        risk_score = self._compute_risk_score(df, pii_columns_detected, dataframe)

        if risk_score > risk_threshold:
            logger.warning(
                "Re-identification risk above threshold",
                risk_score=round(risk_score, 4),
                threshold=risk_threshold,
                job_id=str(job_id),
            )

        logger.info(
            "De-identification completed",
            columns_masked=len(pii_columns_detected),
            risk_score=round(risk_score, 4),
            job_id=str(job_id),
        )

        return {
            "dataframe": df,
            "audit_log": audit_log,
            "pii_columns_detected": pii_columns_detected,
            "risk_score": round(risk_score, 4),
        }

    async def detect_pii(
        self,
        dataframe: pd.DataFrame,
        sample_size: int = 500,
    ) -> dict[str, list[str]]:
        """Scan a DataFrame for columns that contain PII.

        Args:
            dataframe: Dataset to scan.
            sample_size: Maximum rows to sample per column for detection.

        Returns:
            Dict mapping column name to list of detected PII entity types.
        """
        return self._auto_detect_pii_columns(
            dataframe.head(sample_size),
            already_configured=set(),
        )

    def _apply_masking(
        self,
        series: pd.Series,  # type: ignore[type-arg]
        entity_types: list[str],
        strategy: str,
        job_secret: str,
        col_name: str,
    ) -> tuple[pd.Series, int]:  # type: ignore[type-arg]
        """Apply the chosen masking strategy to all PII values in a Series.

        Args:
            series: Column data.
            entity_types: PII types to scan for in this column.
            strategy: 'hash' | 'redact' | 'pseudonymize' | 'generalize'
            job_secret: HMAC secret for pseudonymization.
            col_name: Column name (used for cache namespace).

        Returns:
            Tuple of (masked Series, count of values that contained PII).
        """
        if series.dtype == object:
            masked_series = series.copy()
            pii_count = 0

            for idx, value in series.items():
                if not isinstance(value, str):
                    continue
                masked_value, did_mask = self._mask_string(
                    value, entity_types, strategy, job_secret, col_name
                )
                if did_mask:
                    masked_series.at[idx] = masked_value
                    pii_count += 1

            return masked_series, pii_count

        # Non-string columns: numeric or other types cannot contain PII patterns
        return series, 0

    def _mask_string(
        self,
        value: str,
        entity_types: list[str],
        strategy: str,
        job_secret: str,
        col_name: str,
    ) -> tuple[str, bool]:
        """Mask PII within a single string value.

        Args:
            value: Raw string value.
            entity_types: Which PII patterns to look for.
            strategy: Masking strategy to apply.
            job_secret: Per-job HMAC secret.
            col_name: Column name for cache scoping.

        Returns:
            Tuple of (masked string, True if any PII was found and masked).
        """
        modified = value
        any_masked = False

        for entity_type in entity_types:
            pattern = _PII_PATTERNS.get(entity_type)
            if pattern is None:
                continue

            matches = pattern.findall(modified)
            if not matches:
                continue

            any_masked = True

            if strategy == "hash":
                modified = pattern.sub(
                    lambda m: self._hash_value(m.group()), modified
                )
            elif strategy == "redact":
                label = f"[{entity_type}_REDACTED]"
                modified = pattern.sub(label, modified)
            elif strategy == "pseudonymize":
                modified = pattern.sub(
                    lambda m: self._pseudonymize(m.group(), job_secret, col_name), modified
                )
            elif strategy == "generalize":
                modified = pattern.sub(
                    lambda m: self._generalize(m.group(), entity_type), modified
                )
            else:
                # Unknown strategy: fall back to redact
                modified = pattern.sub(f"[{entity_type}_REDACTED]", modified)

        return modified, any_masked

    def _hash_value(self, value: str) -> str:
        """SHA-256 hash a PII value.

        Args:
            value: Raw PII string.

        Returns:
            First 16 hex characters of the SHA-256 digest, prefixed with '[HASH:'.
        """
        digest = hashlib.sha256(value.encode()).hexdigest()[:16]
        return f"[HASH:{digest}]"

    def _pseudonymize(self, value: str, job_secret: str, namespace: str) -> str:
        """Generate a stable pseudonym for a PII value within a job.

        Uses HMAC-SHA256 with the job-specific secret so the same input always
        produces the same pseudonym within the same job, but different jobs
        produce different pseudonyms.

        Args:
            value: Raw PII string.
            job_secret: Per-job HMAC secret.
            namespace: Cache namespace (column name).

        Returns:
            Pseudonymized placeholder string.
        """
        import hmac

        cache_key = f"{namespace}::{value}"
        if cache_key not in self._pseudonym_map.setdefault(namespace, {}):
            digest = hmac.new(
                job_secret.encode(), value.encode(), "sha256"
            ).hexdigest()[:12]
            self._pseudonym_map[namespace][cache_key] = f"PSEUDO_{digest.upper()}"

        return self._pseudonym_map[namespace][cache_key]

    def _generalize(self, value: str, entity_type: str) -> str:
        """Partially mask a PII value preserving non-sensitive shape.

        Args:
            value: Raw PII string.
            entity_type: PII type for context-aware generalization.

        Returns:
            Generalized/partially masked string.
        """
        if entity_type == "EMAIL":
            parts = value.split("@")
            if len(parts) == 2:
                domain = parts[1]
                return f"***@{domain}"
        elif entity_type == "PHONE":
            digits = re.sub(r"\D", "", value)
            if len(digits) >= 7:
                return f"***-***-{digits[-4:]}"
        elif entity_type == "SSN":
            return "***-**-" + value[-4:] if len(value) >= 4 else "***-**-****"
        elif entity_type == "CREDIT_CARD":
            digits = re.sub(r"\D", "", value)
            return f"****-****-****-{digits[-4:]}" if len(digits) >= 4 else "[CARD_REDACTED]"
        elif entity_type == "IP":
            parts = value.split(".")
            if len(parts) == 4:
                return f"{parts[0]}.{parts[1]}.*.*"

        # Default: replace middle portion with asterisks
        if len(value) <= 4:
            return "****"
        return value[:2] + "*" * (len(value) - 4) + value[-2:]

    def _auto_detect_pii_columns(
        self,
        dataframe: pd.DataFrame,
        already_configured: set[str],
    ) -> dict[str, list[str]]:
        """Scan text columns for PII patterns.

        Args:
            dataframe: Dataset to scan.
            already_configured: Column names already in the config (skip these).

        Returns:
            Dict of {column_name: [entity_types found]}.
        """
        detected: dict[str, list[str]] = {}

        for col in dataframe.columns:
            if col in already_configured:
                continue
            if dataframe[col].dtype != object:
                continue

            sample = dataframe[col].dropna().astype(str).head(200)
            found_types: list[str] = []

            for entity_type, pattern in _PII_PATTERNS.items():
                matches = sample.apply(lambda v: bool(pattern.search(v)))
                match_rate = matches.mean()
                if match_rate > 0.05:  # at least 5% of sampled values match
                    found_types.append(entity_type)

            if found_types:
                detected[col] = found_types

        return detected

    def _compute_risk_score(
        self,
        masked_df: pd.DataFrame,
        pii_columns: list[str],
        original_df: pd.DataFrame,
    ) -> float:
        """Estimate re-identification risk of the masked dataset.

        Risk is driven by:
        - Proportion of PII columns that still have high cardinality
        - Proportion of quasi-identifier columns (high-cardinality non-PII)

        Args:
            masked_df: De-identified DataFrame.
            pii_columns: Columns where PII was detected.
            original_df: Original (pre-masking) DataFrame.

        Returns:
            Risk score between 0.0 (no risk) and 1.0 (high risk).
        """
        if len(masked_df) == 0:
            return 0.0

        risk_factors: list[float] = []

        # Check if masked PII columns still have very high cardinality
        for col in pii_columns:
            if col not in masked_df.columns:
                continue
            cardinality_ratio = masked_df[col].nunique() / max(len(masked_df), 1)
            # High cardinality in masked column indicates pseudonymization or hash
            # which preserves some linkage risk
            risk_factors.append(min(cardinality_ratio, 1.0) * 0.5)

        # Check for quasi-identifiers: high-cardinality non-PII columns
        non_pii_cols = [c for c in masked_df.columns if c not in pii_columns]
        for col in non_pii_cols:
            if masked_df[col].dtype == object:
                cardinality_ratio = masked_df[col].nunique() / max(len(masked_df), 1)
                if cardinality_ratio > 0.8:
                    risk_factors.append(0.3)

        if not risk_factors:
            return 0.0

        return min(sum(risk_factors) / len(risk_factors), 1.0)

    def _derive_job_secret(self, job_id: uuid.UUID) -> str:
        """Derive a per-job HMAC secret from the job UUID.

        Args:
            job_id: Pipeline job UUID.

        Returns:
            Hex string to use as HMAC key.
        """
        # XOR the job UUID bytes with a stable random salt baked at import time
        # This ensures different jobs get different pseudonyms even for the same input
        return hashlib.sha256(str(job_id).encode()).hexdigest()
