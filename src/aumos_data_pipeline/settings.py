"""Service-specific settings extending AumOS base config."""

from pydantic_settings import SettingsConfigDict

from aumos_common.config import AumOSSettings


class Settings(AumOSSettings):
    """Configuration for the aumos-data-pipeline service.

    All standard AumOS settings (database, kafka, redis, jwt) are inherited.
    Pipeline-specific settings use the AUMOS_DPL_ prefix.
    """

    service_name: str = "aumos-data-pipeline"

    # Storage
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "aumos-data"

    # Airflow orchestration
    airflow_url: str = "http://localhost:8080"
    airflow_username: str = "aumos"
    airflow_password: str = "aumos_dev"

    # DVC versioning
    dvc_remote_url: str = "s3://aumos-data/dvc"
    dvc_remote_endpoint_url: str = "http://localhost:9000"

    # Pipeline limits and thresholds
    max_file_size_mb: int = 1024
    batch_size: int = 10_000
    quality_score_threshold: float = 0.8
    default_parquet_compression: str = "snappy"

    model_config = SettingsConfigDict(env_prefix="AUMOS_DPL_")
