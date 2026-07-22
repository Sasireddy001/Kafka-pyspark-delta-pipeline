output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "s3_bucket" {
  description = "S3 bucket for Delta Lake data and checkpoints"
  value       = aws_s3_bucket.data.id
}

output "msk_cluster_name" {
  description = "MSK serverless cluster name"
  value       = aws_msk_serverless_cluster.kafka.cluster_name
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap brokers (TLS/IAM)"
  value       = aws_msk_serverless_cluster.kafka.bootstrap_brokers_sasl_iam
  sensitive   = true
}

output "emr_application_id" {
  description = "EMR Serverless application ID"
  value       = aws_emrserverless_application.streaming.id
}

output "emr_execution_role_arn" {
  description = "IAM role ARN for EMR Serverless job runs"
  value       = aws_iam_role.emr_execution.arn
}
