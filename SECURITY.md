# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a Vulnerability

Report security vulnerabilities to: security@aumos.ai

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will respond within 48 hours and aim to patch critical issues within 7 days.

## Security Considerations for Data Pipeline

- All dataset storage uses pre-signed URLs with short expiry (15 min)
- Data at rest encrypted via MinIO/S3 server-side encryption
- RLS enforced on all tenant-scoped tables — tenants cannot access other tenants' pipeline jobs or profiles
- DVC versioning preserves immutable data lineage
- Kafka events include tenant_id for audit trail
- No raw PII logged — data profiles contain statistics only, not sample rows
