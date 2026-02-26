# Contributing to aumos-data-pipeline

Thank you for contributing to the AumOS Data Pipeline service.

## Development Setup

```bash
git clone <repo-url>
cd aumos-data-pipeline
cp .env.example .env
pip install -e ".[dev]"
```

## Running Tests

```bash
make test          # Full test suite with coverage
make test-quick    # Fast run, stops on first failure
```

## Code Quality

```bash
make lint          # Ruff linting
make format        # Auto-format with ruff
make typecheck     # mypy strict checks
```

## Commit Conventions

Follow Conventional Commits:
- `feat:` — new feature
- `fix:` — bug fix
- `refactor:` — code restructuring
- `test:` — adding/updating tests
- `docs:` — documentation only
- `chore:` — maintenance, deps

## Architecture Rules

1. All business logic in `core/services.py` — not in routes
2. All external I/O in `adapters/` — not in services
3. Use `aumos_common` for auth, DB, events, logging — never reimplement
4. Type hints on every function signature
5. Table prefix: `dpl_`

## Pull Request Process

1. Branch from `main`: `feature/your-feature`
2. Add tests alongside implementation
3. Ensure `make all` passes
4. Request review from a maintainer
