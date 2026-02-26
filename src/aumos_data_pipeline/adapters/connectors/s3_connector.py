"""S3/MinIO source connector.

Ingests Parquet or CSV files from S3-compatible object storage.
Supports prefix-based multi-file ingestion and streaming read.
"""

from typing import Any, AsyncIterator

import boto3
import pandas as pd
import pyarrow.parquet as pq

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import IngestorProtocol

logger = get_logger(__name__)


class S3Connector:
    """Ingests data from S3/MinIO object storage.

    Implements IngestorProtocol. Supports single-file and prefix-based
    (multi-file) ingestion of Parquet and CSV formats.

    Expected source_config keys:
        endpoint_url: S3/MinIO endpoint URL
        access_key: Access key / username
        secret_key: Secret key / password
        bucket: S3 bucket name
        key: Single object key (exclusive with prefix)
        prefix: Object key prefix for multi-file ingestion (exclusive with key)
        format: 'parquet' | 'csv' (default: auto-detect from extension)
        region: AWS region (default: us-east-1)
    """

    async def ingest(
        self,
        source_config: dict[str, Any],
        chunk_size: int = 10_000,
    ) -> AsyncIterator[pd.DataFrame]:
        """Stream data from S3/MinIO in chunks.

        Args:
            source_config: S3 connection and object configuration.
            chunk_size: Rows per chunk for CSV reads.

        Yields:
            DataFrame chunks.
        """
        s3_client = boto3.client(
            "s3",
            endpoint_url=source_config.get("endpoint_url"),
            aws_access_key_id=source_config["access_key"],
            aws_secret_access_key=source_config["secret_key"],
            region_name=source_config.get("region", "us-east-1"),
        )

        bucket = source_config["bucket"]
        keys: list[str] = []

        if "key" in source_config:
            keys = [source_config["key"]]
        elif "prefix" in source_config:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=source_config["prefix"]):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])

        if not keys:
            logger.warning("No objects found in S3", bucket=bucket, config=source_config)
            return

        for key in keys:
            logger.info("Reading S3 object", bucket=bucket, key=key)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()

            file_format = source_config.get("format", "")
            if not file_format:
                file_format = "parquet" if key.endswith(".parquet") else "csv"

            if file_format == "parquet":
                import io  # noqa: PLC0415

                table = pq.read_table(io.BytesIO(body))
                dataframe = table.to_pandas()
                # Yield in chunks
                for i in range(0, len(dataframe), chunk_size):
                    yield dataframe.iloc[i : i + chunk_size].copy()
            else:
                import io  # noqa: PLC0415

                for chunk in pd.read_csv(io.BytesIO(body), chunksize=chunk_size):
                    yield chunk

    async def validate_connection(self, source_config: dict[str, Any]) -> bool:
        """Test that the S3/MinIO bucket is accessible.

        Args:
            source_config: Must include endpoint_url, access_key, secret_key, bucket.

        Returns:
            True if bucket HEAD request succeeds.
        """
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=source_config.get("endpoint_url"),
                aws_access_key_id=source_config["access_key"],
                aws_secret_access_key=source_config["secret_key"],
                region_name=source_config.get("region", "us-east-1"),
            )
            s3_client.head_bucket(Bucket=source_config["bucket"])
            return True
        except Exception as exc:
            logger.warning("S3 connection validation failed", error=str(exc))
            return False
