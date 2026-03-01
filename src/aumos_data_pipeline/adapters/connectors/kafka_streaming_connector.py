"""Kafka streaming source connector for aumos-data-pipeline (GAP-117).

Consumes messages from a Kafka topic using aiokafka, accumulates micro-batches,
and stages them to MinIO as Parquet for pipeline processing.
"""

from __future__ import annotations

import asyncio
import io
import json
import uuid
from typing import Any

import pandas as pd

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class KafkaStreamingConnector:
    """Ingests data from a Kafka topic via micro-batch accumulation.

    Consumes messages from a Kafka topic using aiokafka, accumulates rows
    until the batch_size threshold or timeout_seconds is reached, then
    stages the batch to MinIO as Parquet for downstream pipeline processing.

    Credentials (bootstrap_servers, sasl_username, sasl_password) are
    resolved from aumos-secrets-vault.

    Args:
        topic: Kafka topic name to consume from.
        group_id: Consumer group ID for offset tracking.
        credentials_secret_id: Secret ID for Kafka credentials.
        secrets_client: Secrets vault client.
        batch_size: Maximum messages per micro-batch (default 1000).
        timeout_seconds: Maximum seconds to wait for a full batch (default 30).
        auto_offset_reset: Offset reset policy: 'earliest' or 'latest'.
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        credentials_secret_id: str,
        secrets_client: Any,
        batch_size: int = 1000,
        timeout_seconds: float = 30.0,
        auto_offset_reset: str = "latest",
    ) -> None:
        """Initialize the Kafka streaming connector.

        Args:
            topic: Kafka topic name.
            group_id: Consumer group ID.
            credentials_secret_id: Secret ID for Kafka connection credentials.
            secrets_client: Secrets vault client.
            batch_size: Messages per micro-batch.
            timeout_seconds: Maximum wait time per batch.
            auto_offset_reset: 'earliest' or 'latest'.
        """
        self._topic = topic
        self._group_id = group_id
        self._credentials_secret_id = credentials_secret_id
        self._secrets_client = secrets_client
        self._batch_size = batch_size
        self._timeout_seconds = timeout_seconds
        self._auto_offset_reset = auto_offset_reset

    async def ingest_batch(
        self,
        staging_storage: Any,
        tenant_id: uuid.UUID,
        job_id: uuid.UUID,
    ) -> "StreamingBatchResult":
        """Consume one micro-batch from Kafka and stage as Parquet.

        Reads up to batch_size messages within timeout_seconds from the
        configured topic. Deserializes each message value as JSON. Any
        non-JSON messages are logged as warnings and skipped.

        Args:
            staging_storage: Storage adapter with upload() method.
            tenant_id: Owning tenant UUID.
            job_id: Pipeline job UUID.

        Returns:
            StreamingBatchResult with output URI, message count, and schema.
        """
        from aumos_data_pipeline.adapters.connectors.snowflake_connector import IngestResult

        credentials = await self._secrets_client.get(self._credentials_secret_id)
        bootstrap_servers: str = credentials.get("bootstrap_servers", "localhost:9092")
        sasl_username: str = credentials.get("sasl_username", "")
        sasl_password: str = credentials.get("sasl_password", "")

        logger.info(
            "kafka_batch_ingest_started",
            topic=self._topic,
            group_id=self._group_id,
            batch_size=self._batch_size,
            job_id=str(job_id),
        )

        records: list[dict[str, Any]] = []

        try:
            from aiokafka import AIOKafkaConsumer  # type: ignore[import]

            consumer_kwargs: dict[str, Any] = {
                "bootstrap_servers": bootstrap_servers,
                "group_id": self._group_id,
                "auto_offset_reset": self._auto_offset_reset,
                "enable_auto_commit": True,
                "value_deserializer": lambda v: v,
            }

            if sasl_username and sasl_password:
                consumer_kwargs.update(
                    {
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_plain_username": sasl_username,
                        "sasl_plain_password": sasl_password,
                    }
                )

            consumer = AIOKafkaConsumer(self._topic, **consumer_kwargs)
            await consumer.start()

            try:
                deadline = asyncio.get_event_loop().time() + self._timeout_seconds

                while len(records) < self._batch_size:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        break

                    try:
                        message = await asyncio.wait_for(
                            consumer.__anext__(),
                            timeout=min(remaining, 5.0),
                        )
                        try:
                            value = json.loads(message.value.decode("utf-8"))
                            if isinstance(value, dict):
                                records.append(value)
                            else:
                                records.append({"value": value})
                        except (json.JSONDecodeError, UnicodeDecodeError) as parse_err:
                            logger.warning(
                                "kafka_message_parse_error",
                                topic=self._topic,
                                error=str(parse_err),
                            )
                    except asyncio.TimeoutError:
                        break
            finally:
                await consumer.stop()

        except ImportError:
            logger.warning(
                "aiokafka_not_installed",
                message="aiokafka not installed; returning empty batch",
            )

        df = pd.DataFrame(records) if records else pd.DataFrame()

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        key = f"dpl-staging/{tenant_id}/{job_id}/kafka_batch_{uuid.uuid4().hex[:8]}.parquet"
        uri = await staging_storage.upload(buf.read(), key)

        logger.info(
            "kafka_batch_ingest_completed",
            topic=self._topic,
            message_count=len(records),
            job_id=str(job_id),
            output_uri=uri,
        )

        return StreamingBatchResult(
            output_uri=uri,
            message_count=len(records),
            topic=self._topic,
            schema={col: str(dtype) for col, dtype in df.dtypes.items()},
        )


class StreamingBatchResult:
    """Result of a Kafka micro-batch ingestion.

    Attributes:
        output_uri: Storage URI of the staged Parquet file.
        message_count: Number of Kafka messages consumed.
        topic: Source Kafka topic name.
        schema: Column name to dtype mapping.
    """

    def __init__(
        self,
        output_uri: str,
        message_count: int,
        topic: str,
        schema: dict[str, str],
    ) -> None:
        """Initialize StreamingBatchResult.

        Args:
            output_uri: Storage URI of staged Parquet.
            message_count: Number of messages consumed.
            topic: Source Kafka topic.
            schema: Column dtype mapping.
        """
        self.output_uri = output_uri
        self.message_count = message_count
        self.topic = topic
        self.schema = schema
