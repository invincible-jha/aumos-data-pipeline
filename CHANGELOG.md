# Changelog

All notable changes to `aumos-data-pipeline` are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-02-26

### Added
- Initial scaffolding for AumOS Data Pipeline service
- Multi-source ingestion: PostgreSQL, S3/MinIO, CSV/Parquet, REST API, streaming
- Automatic data profiling: types, distributions, missing values, outliers
- Data cleaning: deduplication, normalization, imputation, encoding, outlier handling
- Feature engineering and format conversion (CSV → Parquet, JSON → Arrow)
- Great Expectations quality gate integration
- DVC data versioning for reproducibility
- Full hexagonal architecture (api/ + core/ + adapters/)
- SQLAlchemy models: PipelineJob, DataProfile, QualityCheck with tenant isolation
- FastAPI REST API with pipeline orchestration endpoints
- Protocol-based interfaces for all pipeline stages
- Kafka event publishing for pipeline lifecycle events
- Docker multi-stage build and docker-compose.dev.yml
- CI/CD GitHub Actions workflow
