# aumos-data-pipeline

**AumOS Data Engineering Pipeline** — multi-source ingestion, automatic profiling,
data cleaning, feature transformation, and Great Expectations quality gates for
AI-ready data delivery.

Part of **AumOS Enterprise** — Tier B (Open Core), Product: Data Factory.

## Overview

`aumos-data-pipeline` is the data engineering backbone of the AumOS platform. It
transforms raw data from any source (PostgreSQL, S3, CSV, REST API, streaming) into
clean, profiled, versioned datasets ready for downstream AI workloads like tabular
synthesis, model training, and federated learning.

### Pipeline Stages

```
Source → [Ingest] → [Profile] → [Clean] → [Transform] → [Quality Gate] → Output
             ↓          ↓          ↓           ↓               ↓
          PipelineJob  DataProfile  PipelineJob  PipelineJob  QualityCheck
             ↓                                                    ↓
          Kafka events                                        DVC versioning
```

## Architecture

```
src/aumos_data_pipeline/
├── main.py                        # FastAPI entry point
├── settings.py                    # Pydantic settings (extends AumOSSettings)
├── api/
│   ├── router.py                  # REST endpoints
│   └── schemas.py                 # Pydantic request/response models
├── core/
│   ├── models.py                  # SQLAlchemy ORM: PipelineJob, DataProfile, QualityCheck
│   ├── interfaces.py              # Protocols: IngestorProtocol, ProfilerProtocol, etc.
│   └── services.py                # Business logic: 6 services
└── adapters/
    ├── connectors/
    │   ├── postgres_connector.py  # PostgreSQL source
    │   ├── s3_connector.py        # S3/MinIO source
    │   ├── csv_connector.py       # CSV/Parquet file source
    │   └── api_connector.py       # REST API source
    ├── profiler.py                # pandas/polars profiling engine
    ├── cleaner.py                 # Data cleaning pipeline
    ├── transformer.py             # Feature engineering & format conversion
    ├── quality_gate.py            # Great Expectations integration
    ├── dvc_versioner.py           # DVC data versioning
    ├── repositories.py            # SQLAlchemy repositories
    ├── kafka.py                   # Pipeline event publishing
    └── storage.py                 # MinIO/S3 artifact storage
```

## Quick Start

```bash
cp .env.example .env
pip install -e ".[dev]"
make docker-run       # Start Postgres, Redis, MinIO, Kafka
uvicorn aumos_data_pipeline.main:app --reload
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/pipeline/ingest` | Ingest data from a source |
| POST | `/api/v1/pipeline/profile` | Profile an existing dataset |
| POST | `/api/v1/pipeline/clean` | Clean and normalize data |
| POST | `/api/v1/pipeline/transform` | Feature engineering / format conversion |
| POST | `/api/v1/pipeline/orchestrate` | Run full pipeline end-to-end |
| GET | `/api/v1/pipeline/jobs/{id}` | Get pipeline job status |
| GET | `/api/v1/pipeline/profiles/{id}` | Get data profile results |

## Environment Variables

See `.env.example` for all required configuration.

Key variables:
- `DATABASE_URL` — PostgreSQL connection string
- `MINIO_ENDPOINT` — MinIO/S3 endpoint for dataset storage
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka broker addresses
- `DVC_REMOTE_URL` — DVC remote for data versioning

## Development

```bash
make install      # Install with dev dependencies
make test         # Run tests (80% coverage required)
make lint         # Ruff linting
make typecheck    # mypy strict
make format       # Auto-format
```

## Table Prefix

All database tables use prefix `dpl_`:
- `dpl_pipeline_jobs`
- `dpl_data_profiles`
- `dpl_quality_checks`

## License

Apache 2.0 — see [LICENSE](LICENSE).
