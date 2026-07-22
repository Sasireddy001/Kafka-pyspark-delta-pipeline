# Monitoring Guide — Kafka → PySpark → Delta Lake on AWS

This guide explains what is monitored, how to access the dashboards, and how to respond to common alerts.

## What is Provisioned

The `cloudwatch.tf` Terraform module creates:

- **CloudWatch Log Group** for the EMR Serverless streaming job (`/aws/emr-serverless/{application-id}/streaming`).
- **SNS Topic** for alerts.
- **Optional email subscription** if `alert_email` is configured.
- **CloudWatch Alarms** for:
  - EMR Serverless job failures
  - MSK consumer lag
  - S3 4xx errors on the data bucket

## Key Metrics

| Metric | Namespace | Threshold | Action |
|---|---|---|---|
| `JobsFailed` | `AWS/EMRServerless` | > 0 in 5 min | Investigate job logs and replay from checkpoint |
| `SumOffsetLag` | `AWS/Kafka` | > 1000 for 10 min | Scale consumers or reduce ingestion rate |
| `4xxErrors` | `AWS/S3` | > 0 in 5 min | Check IAM permissions and bucket policy |
| Log `ERROR` messages | `Logs` | > 0 in 5 min | Inspect the CloudWatch log group |

## Accessing the Dashboards

1. Open the [CloudWatch Console](https://console.aws.amazon.com/cloudwatch/).
2. Choose **Dashboards** and create a custom dashboard named `kafka-pyspark-delta-{env}`.
3. Add widgets for:
   - **EMR Serverless:** `JobsRunning`, `JobsFailed`, `JobsSubmitted`
   - **MSK:** `MessagesInPerSec`, `SumOffsetLag`, `EstimatedMaxProducerCount`
   - **S3:** `BucketSizeBytes`, `NumberOfObjects`, `4xxErrors`
   - **Logs:** Insights query for `fields @timestamp, @message | filter @message like /ERROR/`

## Responding to Alerts

### EMR job failure
1. Go to **EMR → Serverless → Applications → {app} → Job runs**.
2. Find the failed job and open the **CloudWatch logs**.
3. Check for schema mismatch, Kafka connectivity, or Delta path errors.
4. Fix the issue and resubmit from the checkpoint location.

### High consumer lag
1. Open the **CloudWatch → Metrics → AWS/Kafka**.
2. Filter by `Cluster Name` and `Consumer Group`.
3. If lag is rising, increase the number of Spark micro-batch executors or add Kafka partitions.

### S3 4xx errors
1. Check the IAM execution role has `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` on the data bucket.
2. Verify the bucket policy does not block EMR or VPC traffic.
3. Check for expired credentials or missing KMS permissions.

## Observability Best Practices

- Keep CloudWatch log retention to 7 days in dev, 30–90 days in prod.
- Use structured logging (`JSON`) from the streaming job to make log queries easier.
- Add custom metrics (e.g., events processed per batch) using `spark.metrics` or `CloudWatch PutMetricData`.
- Export key dashboards to the portfolio and project README as screenshots after the first deployment.
