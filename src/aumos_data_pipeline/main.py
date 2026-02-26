"""AumOS Data Pipeline service entry point."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from aumos_common.app import create_app
from aumos_common.database import init_database
from aumos_common.health import HealthCheck
from aumos_common.observability import get_logger

from aumos_data_pipeline.settings import Settings

logger = get_logger(__name__)
settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage service lifecycle — startup and shutdown."""
    # Startup
    logger.info("Starting aumos-data-pipeline", version="0.1.0", environment=settings.environment)
    init_database(settings.database)
    # TODO: Initialize Kafka publisher
    # TODO: Initialize MinIO client
    # TODO: Initialize DVC remote
    yield
    # Shutdown
    logger.info("Shutting down aumos-data-pipeline")


app = create_app(
    service_name="aumos-data-pipeline",
    version="0.1.0",
    settings=settings,
    lifespan=lifespan,
    health_checks=[
        # HealthCheck(name="postgres", check_fn=check_db),
        # HealthCheck(name="minio", check_fn=check_minio),
    ],
)

# Import and include routers
from aumos_data_pipeline.api.router import router  # noqa: E402

app.include_router(router, prefix="/api/v1")
