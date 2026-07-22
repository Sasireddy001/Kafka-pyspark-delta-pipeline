# AWS Cloud Deployment — Kafka → PySpark → Delta Lake

Production-ready Terraform deployment for the Kafka → PySpark → Delta Lake pipeline on AWS. This uses **Amazon MSK Serverless** for Kafka, **Amazon EMR Serverless** for Spark processing, and **Amazon S3** for Delta Lake storage and checkpoints.

## Architecture

```mermaid
flowchart TB
    subgraph Producers
        P[Microservices / IoT / Test Generator]
    end

    subgraph Streaming
        MSK[Amazon MSK Serverless]
    end

    subgraph Compute
        EMR[Amazon EMR Serverless Spark]
        SJ[streaming_job.py]
    end

    subgraph Storage
        S3[(Amazon S3)]
        DL[Delta Lake Bronze]
        CP[Checkpoints]
    end

    P -->|TLS/IAM| MSK
    MSK -->|readStream| EMR
    EMR --> SJ
    SJ -->|writeStream| S3
    S3 --> DL
    SJ -->|offsets| CP
```

## What is provisioned

| Resource | Purpose |
|---|---|
| VPC + public subnets | Network isolation for MSK and EMR |
| Amazon MSK Serverless | Managed Kafka with IAM authentication |
| Amazon S3 bucket | Delta Lake data + Spark checkpoints |
| IAM role | Least-privilege execution role for EMR Serverless |
| Amazon EMR Serverless | Spark application for the streaming job |
| S3 job object | The `src/pipeline/streaming_job.py` uploaded as a deployment artifact |

## Prerequisites

- AWS account
- AWS CLI configured with credentials
- Terraform >= 1.5.0
- GitHub repository secrets set (for CI/CD):
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_REGION`

## Deployment Steps

1. **Configure variables**

   ```bash
   cd deploy/aws
   cp terraform.tfvars.example terraform.tfvars
   # edit terraform.tfvars as needed
   ```

2. **Initialize and plan**

   ```bash
   terraform init
   terraform plan -out=tfplan
   ```

3. **Apply**

   ```bash
   terraform apply tfplan
   ```

4. **Get outputs**

   ```bash
   terraform output
   ```

   Note `msk_bootstrap_brokers`, `emr_application_id`, `s3_bucket`, and `emr_execution_role_arn`.

5. **Submit the Spark streaming job**

   Use the AWS CLI or the provided `spark-submit.sh` template:

   ```bash
   aws emr-serverless start-job-run \
     --application-id <emr_application_id> \
     --execution-role-arn <emr_execution_role_arn> \
     --job-driver '{
       "sparkSubmit": {
         "entryPoint": "s3://<bucket>/jobs/streaming_job.py",
         "entryPointArguments": [],
         "sparkSubmitParameters": "--conf spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
       }
     }'
   ```

6. **Monitor**

   - EMR Serverless job runs: AWS Console → EMR → Serverless → Applications
   - S3 data: `<bucket>/delta/events/`
   - CloudWatch Logs: `/aws/emr-serverless/jobs/<job-id>`

## CI/CD

The `.github/workflows/aws-deploy.yml` workflow runs:

1. `terraform fmt -check`
2. `terraform validate`
3. `terraform plan` on pull requests
4. `terraform apply` on `main` (requires approval for production)

## Cost Considerations

- MSK Serverless bills per GB-hour and partition-hour.
- EMR Serverless bills per worker-hour; use auto-stop and scheduled job runs to minimize cost.
- S3 charges for storage and requests; set lifecycle policies to archive old checkpoints.
- Use `terraform destroy` to clean up dev resources when not in use.

## Security

- MSK uses IAM authentication over TLS (`IAM_AUTH` on port 9098).
- S3 bucket has server-side encryption and versioning enabled.
- EMR job execution role follows least privilege (S3 + MSK access only).
- All resources are deployed into a dedicated VPC with security groups.

## Future Improvements

- Add VPC Flow Logs and CloudTrail for audit.
- Replace public subnets with private subnets + NAT Gateway for production.
- Add Amazon CloudWatch alarms for lag and job failure.
- Use AWS Secrets Manager for MSK credentials if SASL/SCRAM is required.
