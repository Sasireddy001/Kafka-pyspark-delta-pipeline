# Changelog

## [1.1.0] — 2026-07-22

### Added
- AWS cloud deployment with Terraform: MSK Serverless, EMR Serverless, S3, and CI/CD workflow.
- `ARCHITECTURE.md` with high-level design, component diagram, scalability, fault tolerance, and deployment model.
- `SYSTEM_DESIGN.md` with requirements, functional design, NFRs, tradeoffs, and design decisions.
- `deploy/aws/README.md` with step-by-step deployment and cost guidance.

## [1.0.0] — 2026-07-01

### Added
- Initial Kafka → PySpark → Delta Lake streaming pipeline.
- Docker Compose local setup with Kafka and Zookeeper.
- PySpark Structured Streaming with schema enforcement, watermarking, and deduplication.
- `pytest` test suite with in-memory Spark fixture.
- GitHub Actions CI for linting and tests.
- Throughput benchmark (`31k–45k rows/s` on a 4-core laptop).
