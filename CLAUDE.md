# CLAUDE.md — AumOS Data Pipeline

## Project Overview

AumOS Enterprise is a composable enterprise AI platform with 9 products + 2 services
across 62 repositories. This repo (`aumos-data-pipeline`) is part of **Tier B: Open Core**:
Data engineering pipeline for AI-ready data delivery.

**Release Tier:** B: Open Core
**Product Mapping:** Product 3 — Data Factory
**Phase:** 1B (Months 5-10)

## Repo Purpose

`aumos-data-pipeline` provides the data engineering backbone for the AumOS platform.
It ingests raw data from multiple sources (PostgreSQL, S3, CSV, REST API, streaming),
profiles it automatically, cleans and normalizes it, applies feature transformations,
and validates quality via Great Expectations before delivering AI-ready datasets to
downstream synthesis and model training services.

## Architecture Position

```
aumos-platform-core → aumos-data-layer → aumos-data-pipeline → aumos-tabular-engine
                   → aumos-event-bus (publishes pipeline lifecycle events)
                   → aumos-fidelity-validator (sends profiled datasets for validation)
                   → aumos-privacy-engine (routes PII data for de-identification)
```

**Upstream dependencies (this repo IMPORTS from):**
- `aumos-common` — auth, database, events, errors, config, health, pagination
- `aumos-proto` — Protobuf message definitions for Kafka events
- `aumos-data-layer` — shared data lake storage abstraction

**Downstream dependents (other repos IMPORT from this):**
- `aumos-tabular-engine` — consumes profiled/cleaned datasets for synthesis
- `aumos-fidelity-validator` — consumes data profiles for statistical fidelity assessment
- `aumos-privacy-engine` — receives column profiles to detect PII columns

## Tech Stack (DO NOT DEVIATE)

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11+ | Runtime |
| FastAPI | 0.110+ | REST API framework |
| SQLAlchemy | 2.0+ (async) | Database ORM |
| asyncpg | 0.29+ | PostgreSQL async driver |
| Pydantic | 2.6+ | Data validation, settings, API schemas |
| pandas | 2.2+ | Data profiling and cleaning |
| polars | 0.20+ | High-performance data transformation |
| pyarrow | 15.0+ | Parquet/Arrow format I/O |
| great-expectations | 0.18+ | Data quality validation |
| dvc | 3.42+ | Data versioning and reproducibility |
| boto3 | 1.34+ | S3/MinIO object storage |
| scikit-learn | 1.4+ | Feature engineering (encoding, scaling) |
| confluent-kafka | 2.3+ | Kafka producer/consumer |
| structlog | 24.1+ | Structured JSON logging |
| OpenTelemetry | 1.23+ | Distributed tracing |
| pytest | 8.0+ | Testing framework |
| ruff | 0.3+ | Linting and formatting |
| mypy | 1.8+ | Type checking |

## Coding Standards

### ABSOLUTE RULES (violations will break integration with other repos)

1. **Import aumos-common, never reimplement.** If aumos-common provides it, use it.
   ```python
   # CORRECT
   from aumos_common.auth import get_current_tenant, get_current_user
   from aumos_common.database import get_db_session, Base, AumOSModel, BaseRepository
   from aumos_common.events import EventPublisher, Topics
   from aumos_common.errors import NotFoundError, ErrorCode
   from aumos_common.config import AumOSSettings
   from aumos_common.health import create_health_router
   from aumos_common.pagination import PageRequest, PageResponse, paginate
   from aumos_common.app import create_app
   ```

2. **Type hints on EVERY function.** No exceptions.

3. **Pydantic models for ALL API inputs/outputs.** Never return raw dicts.

4. **RLS tenant isolation via aumos-common.** Never write raw SQL that bypasses RLS.

5. **Structured logging via structlog.** Never use print() or logging.getLogger().

6. **Publish domain events to Kafka after state changes.**

7. **Async by default.** All I/O operations must be async.

8. **Google-style docstrings** on all public classes and functions.

### File Structure Convention

```
src/aumos_data_pipeline/
├── __init__.py
├── main.py                   # FastAPI app entry point using create_app()
├── settings.py               # Extends AumOSSettings with repo-specific config
├── api/
│   ├── __init__.py
│   ├── router.py             # APIRouter with all endpoints
│   └── schemas.py            # Pydantic request/response models
├── core/
│   ├── __init__.py
│   ├── models.py             # SQLAlchemy ORM models (extend Base, AumOSModel)
│   ├── services.py           # Business logic services
│   └── interfaces.py         # Abstract interfaces (Protocol classes)
├── adapters/
│   ├── __init__.py
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── postgres_connector.py
│   │   ├── s3_connector.py
│   │   ├── csv_connector.py
│   │   └── api_connector.py
│   ├── profiler.py
│   ├── cleaner.py
│   ├── transformer.py
│   ├── quality_gate.py
│   ├── dvc_versioner.py
│   ├── repositories.py
│   ├── kafka.py
│   └── storage.py
└── migrations/
    ├── env.py
    ├── alembic.ini
    └── versions/
```

## API Conventions

- All endpoints under `/api/v1/` prefix
- Auth: Bearer JWT token (validated by aumos-common)
- Tenant: `X-Tenant-ID` header (set by auth middleware)
- Request ID: `X-Request-ID` header (auto-generated if missing)
- Pagination: `?page=1&page_size=20&sort_by=created_at&sort_order=desc`
- Errors: Standard `ErrorResponse` from aumos-common
- Content-Type: `application/json` (always)

## Database Conventions

- Table prefix: `dpl_` (e.g., `dpl_pipeline_jobs`, `dpl_data_profiles`)
- ALL tenant-scoped tables: extend `AumOSModel` (gets id, tenant_id, created_at, updated_at)
- RLS policy on every tenant table (created in migration)
- Foreign keys to other repos: use UUID type, no FK constraints (cross-service)

## Kafka Events

Pipeline publishes to these topics:
- `pipeline.job.created` — when a new job is submitted
- `pipeline.job.completed` — when a job finishes successfully (includes quality_score)
- `pipeline.job.failed` — when a job fails (includes error details)
- `pipeline.profile.completed` — when data profiling completes (includes column metadata)

## Data Versioning (DVC)

- Every output dataset is versioned with DVC
- DVC remote: MinIO S3-compatible storage
- Each PipelineJob stores `data_version` (DVC commit hash) and `output_uri`
- Use `VersioningService` to commit and tag versions — never call DVC directly

## Repo-Specific Context

### Pipeline Job Types
- `ingest` — raw ingestion from source to staging area
- `profile` — statistical profiling of a dataset
- `clean` — deduplication, normalization, imputation
- `transform` — feature engineering, format conversion
- `orchestrate` — full pipeline (ingest → profile → clean → transform → quality gate)

### Performance Requirements
- Profiling: < 30s for datasets up to 1M rows (use polars for large datasets)
- Cleaning: streaming chunked processing for datasets > 100MB
- Format conversion: Arrow columnar for maximum throughput

### Great Expectations Integration
- Expectation suites stored in MinIO at `s3://aumos-data/ge-suites/{tenant_id}/`
- Results stored in QualityCheck table
- Quality score = (passed_expectations / total_expectations)
- Reject threshold configurable via `AUMOS_DPL_QUALITY_SCORE_THRESHOLD` (default 0.8)

### Source Connector Registry
All connectors implement `IngestorProtocol`. Register new connectors in
`IngestionService._connector_registry`. Never call connectors directly from routes.

## What Claude Code Should NOT Do

1. **Do NOT reimplement anything in aumos-common.**
2. **Do NOT use print().** Use `get_logger(__name__)`.
3. **Do NOT return raw dicts from API endpoints.**
4. **Do NOT write raw SQL.** Use SQLAlchemy ORM with BaseRepository.
5. **Do NOT hardcode configuration.** Use Pydantic Settings with env vars.
6. **Do NOT skip type hints.** Every function signature must be typed.
7. **Do NOT load entire datasets into memory.** Use chunked/streaming reads.
8. **Do NOT put business logic in API routes.**
9. **Do NOT call DVC CLI directly.** Use `VersioningService` / `DVCVersioner`.
10. **Do NOT bypass RLS.** Cross-tenant data access requires explicit documentation.
