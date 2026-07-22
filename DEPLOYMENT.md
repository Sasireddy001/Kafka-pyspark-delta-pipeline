# Deployment Guide — Kafka → PySpark → Delta Lake Pipeline

This document is the single source of truth for deploying the streaming pipeline. It covers local development, Databricks, and AWS.

---

## Deployment options

| Environment | Method | Best for |
|---|---|---|
| Local | Docker Compose + Python | Development, unit tests, quick feedback |
| Databricks | Databricks job/cluster | Production-grade streaming on a managed lakehouse |
| AWS | Terraform + EMR Serverless + MSK Serverless | Cloud-native, serverless, recruiter-visible deployment |

---

## Local deployment

**Time to first event:** ~3 minutes  
**Cost:** $0

```bash
# 1. Clone and start Kafka
git clone https://github.com/Sasireddy001/Kafka-pyspark-delta-pipeline.git
cd Kafka-pyspark-delta-pipeline
docker-compose up -d

# 2. Install dependencies
python -m venv .venv
source .venv/bin/activate  # or .\venv\Scripts\activate on Windows
pip install -e ".[dev]"

# 3. Generate sample events
python scripts/generate_sample_events.py --topic events --count 100000

# 4. Run the streaming job
python -m pipeline.streaming_job
```

**Verify output**

```bash
ls data/delta/events/_delta_log/
ls data/delta/events/event_date=*/
```

You should see Parquet files and `_delta_log` JSON transaction files.

---

## Databricks deployment

**Prerequisites:** Databricks workspace, cluster with Spark 3.5, Delta Lake 2.4.

1. Upload `src/pipeline/streaming_job.py` to DBFS or a repo.
2. Create a cluster with the required Maven/PIP libraries:
   - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
   - `io.delta:delta-core_2.12:2.4.0`
3. Create a job with `spark.submit.pyFiles` pointing to the config and utils modules.
4. Run the job. Monitor under **Jobs → Runs** in the Databricks UI.

---

## AWS deployment

See [`deploy/aws/README.md`](deploy/aws/README.md) for the full Terraform setup.

**High-level flow**

1. Configure AWS credentials and `terraform.tfvars`.
2. Run `terraform init`, `plan`, `apply`.
3. Submit the Spark job to EMR Serverless.
4. Verify Delta data in S3 and CloudWatch logs.

**Expected evidence after a successful run**

- MSK cluster status `Available`.
- EMR Serverless job run `Success`.
- S3 bucket contains `delta/events/` and `_delta_log/`.
- CloudWatch log group shows checkpoint commits.
- Terraform outputs for `s3_bucket`, `msk_bootstrap_brokers`, `emr_application_id`.

---

## Monitoring and operations

- CloudWatch dashboard for `JobsFailed`, `SumOffsetLag`, and `S3 4xxErrors`.
- SNS email alerts when `alert_email` is configured.
- Local logs are written to `data/logs/`.

---

## Destroying resources

```bash
# AWS
cd deploy/aws
terraform destroy

# Local
docker-compose down -v
rm -rf data/delta data/checkpoints
```
