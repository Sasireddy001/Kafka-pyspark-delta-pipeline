# -----------------------------------------------------------------------------
# CloudWatch Monitoring for the Streaming Pipeline
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "streaming" {
  name              = "/aws/emr-serverless/${aws_emrserverless_application.streaming.id}/streaming"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-${var.environment}-streaming-logs"
  }
}

resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-alerts"

  tags = {
    Name = "${var.project_name}-${var.environment}-alerts"
  }
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Alarm: any failed EMR Serverless job in a 5-minute window
resource "aws_cloudwatch_metric_alarm" "emr_jobs_failed" {
  alarm_name          = "${var.project_name}-${var.environment}-emr-jobs-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobsFailed"
  namespace           = "AWS/EMRServerless"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when an EMR Serverless streaming job fails"
  dimensions = {
    ApplicationId = aws_emrserverless_application.streaming.id
  }
  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-emr-jobs-failed"
  }
}

# Alarm: Kafka consumer lag remains high for 2 consecutive periods (MSK)
resource "aws_cloudwatch_metric_alarm" "msk_consumer_lag" {
  alarm_name          = "${var.project_name}-${var.environment}-msk-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "SumOffsetLag"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1000
  alarm_description   = "High consumer lag on the Kafka topic"
  dimensions = {
    "Cluster Name" = "${var.project_name}-${var.environment}"
  }
  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-msk-consumer-lag"
  }
}

# Alarm: S3 4xx errors on the data bucket
resource "aws_cloudwatch_metric_alarm" "s3_4xx_errors" {
  alarm_name          = "${var.project_name}-${var.environment}-s3-4xx-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "4xxErrors"
  namespace           = "AWS/S3"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when S3 returns 4xx errors for the data bucket"
  dimensions = {
    BucketName = aws_s3_bucket.data.id
    FilterId = "EntireBucket"
  }
  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Name = "${var.project_name}-${var.environment}-s3-4xx-errors"
  }
}
