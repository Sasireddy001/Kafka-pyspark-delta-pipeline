# Security Policy

## Supported Versions

Only the latest `main` branch of this repository is actively maintained. Security updates will be applied to the most recent commits.

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do not open a public issue.** Public issues can expose vulnerabilities before a fix is available.
2. Email the details to **sasidharmopuru@gmail.com** with the subject line `[Security] Kafka PySpark Delta Pipeline`.
3. Include a clear description, steps to reproduce, and any potential impact.

I will acknowledge the report within 48 hours and aim to provide a resolution or timeline within 7 days.

## Security Practices in This Project

- All credentials and secrets must be injected through environment variables, GitHub Secrets, or AWS IAM roles. They are never hard-coded.
- Terraform state files and `.tfvars` files are excluded from source control.
- S3 buckets have encryption and public-access blocks enabled by default.
- MSK uses IAM authentication over TLS.
- EMR Serverless jobs run with least-privilege IAM execution roles.

## Best Practices for Deployers

- Rotate AWS access keys regularly.
- Use Terraform remote state with encryption and locking.
- Enable AWS CloudTrail for management events.
- Review IAM policies with IAM Access Analyzer before production.
- Run dependency scans (`pip-audit`, `safety`) before each release.
