# Production Checklist — Kafka → PySpark → Delta Lake on AWS

Use this checklist before promoting the pipeline from dev to production.

## Infrastructure

- [ ] S3 bucket has versioning and server-side encryption enabled.
- [ ] S3 bucket blocks public access and uses least-privilege bucket policy.
- [ ] IAM roles use the principle of least privilege; no wildcard `*` actions for S3 or MSK.
- [ ] VPC subnets are private in production; NAT Gateway or AWS PrivateLink is used for outbound access.
- [ ] Security groups restrict MSK and EMR access to the VPC CIDR.
- [ ] Terraform state is stored in a remote backend with state locking.
- [ ] Terraform state bucket is encrypted and versioning is enabled.
- [ ] Resource tags and naming conventions are consistent across environments.

## Kafka / MSK

- [ ] Topic is created with an appropriate partition count for parallelism.
- [ ] Producer clients use IAM authentication over TLS.
- [ ] Topic retention and cleanup policies are configured.
- [ ] Consumer group is named and monitored for lag.
- [ ] Dead-letter queue topic is provisioned for poison-pill events.
- [ ] Schema registry is considered for schema evolution (AWS Glue Schema Registry optional).

## Spark / EMR Serverless

- [ ] `spark-submit` parameters include Delta Lake and Kafka connector packages.
- [ ] `CHECKPOINT_PATH` is on S3 and is unique per environment.
- [ ] `DELTA_PATH` is on S3 and follows the bronze/silver/gold convention.
- [ ] Job has auto-stop and retry policy configured.
- [ ] CloudWatch log group is created and log retention is set.
- [ ] EMR Serverless application network configuration is scoped to private subnets.

## Data Quality

- [ ] JSON schema is enforced at ingestion.
- [ ] Watermark and deduplication settings are tuned to the business SLA.
- [ ] Date/hour partitioning is used for query pruning.
- [ ] Delta Lake `OPTIMIZE` and `VACUUM` are scheduled for the bronze table.
- [ ] Data quality checks (nulls, duplicates, freshness) are in place before silver/gold layers.
- [ ] Bad/late data is routed to a dead-letter path, not silently dropped.

## Monitoring & Alerting

- [ ] CloudWatch dashboard is created for key metrics: lag, throughput, job failures, S3 errors.
- [ ] EMR job failure alarm is wired to an SNS topic or email.
- [ ] MSK consumer lag alarm is configured with a meaningful threshold.
- [ ] S3 4xx/5xx error alarms are active.
- [ ] CloudWatch log metric filters are set for `ERROR` or `FATAL` in the streaming job logs.
- [ ] On-call rotation is documented for alert response.

## Security

- [ ] Secrets are stored in AWS Secrets Manager or parameter store, not in code.
- [ ] `terraform.tfvars` and `.env` are excluded from source control.
- [ ] AWS credentials used by CI/CD are scoped and rotated regularly.
- [ ] Network traffic between MSK, EMR, and S3 stays inside the AWS network.
- [ ] CloudTrail is enabled for management events.

## Cost

- [ ] Dev environment is destroyed when not in use.
- [ ] S3 lifecycle policy archives old checkpoints and data.
- [ ] CloudWatch log retention is set to the minimum needed.
- [ ] Cost anomaly detection and billing alarms are enabled.
- [ ] Reserved capacity or savings plans are evaluated for steady-state production.

## Operations

- [ ] Runbook exists for common failures (job restart, checkpoint corruption, topic replay).
- [ ] Rollback plan is tested: `terraform destroy` and redeploy from last known good state.
- [ ] Backups and restore procedures are documented.
- [ ] Recovery Time Objective (RTO) and Recovery Point Objective (RPO) are defined.
- [ ] CI/CD pipeline is green and gates production deployments.
