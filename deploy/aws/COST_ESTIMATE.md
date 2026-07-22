# AWS Cost Estimate — Kafka → PySpark → Delta Lake

This is a monthly cost estimate for a development environment running on AWS. Production costs scale with data volume, partition count, worker size, and retention.

## Assumptions

| Item | Dev Assumption | Prod Assumption |
|---|---|---|
| Events per day | 1 million | 100 million |
| Average event size | 500 bytes | 500 bytes |
| Streaming hours per day | 4 hours | 24 hours |
| EMR Serverless worker size | 2 vCPU / 8 GB | 4 vCPU / 16 GB |
| MSK Serverless throughput | 200 MB/hr write | 5 GB/hr write |
| S3 storage | 30 days retention | 90 days hot, archive after |

## Estimated Monthly Cost (us-east-1)

| Service | Dev | Prod |
|---|---|---|
| **MSK Serverless** | ~$50–$80 | ~$800–$1,500 |
| **EMR Serverless** | ~$30–$60 | ~$600–$1,200 |
| **S3 Standard** | ~$5 | ~$200–$400 |
| **S3 API requests** | ~$1–$3 | ~$20–$50 |
| **CloudWatch Logs** | ~$5 | ~$50–$100 |
| **CloudWatch Alarms** | ~$1 | ~$5–$10 |
| **SNS** | ~$0–$2 | ~$5–$15 |
| **NAT Gateway** (if private subnets) | ~$30–$35 | ~$30–$35 |
| **Estimated Total** | **~$120–$200** | **~$1,700–$3,300** |

## Cost Optimization Tips

1. **Schedule dev clusters.** Use `terraform destroy` when not actively testing; dev is not 24x7.
2. **Use EMR Serverless auto-stop.** Jobs stop billing when idle; this is already the default.
3. **Archive old data.** Move partitions older than 30 days to S3 Glacier or S3 Infrequent Access.
4. **Reduce log retention.** Set CloudWatch log retention to 7 days in dev.
5. **Right-size MSK partitions.** Start with one partition per broker and scale only when throughput demands it.
6. **Avoid NAT Gateway in dev.** Use public subnets for proof-of-concept; add NAT Gateway only in production.

## Notes

- This is an estimate. Actual costs depend on traffic, region, and reserved capacity.
- Use `terraform destroy` after testing to avoid runaway charges.
- Enable AWS Cost Anomaly Detection and billing alerts for the account.
