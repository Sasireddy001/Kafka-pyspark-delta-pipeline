terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-${var.environment}-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-${var.environment}-igw"
  }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-${var.environment}-public-${count.index + 1}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "msk" {
  name_prefix = "${var.project_name}-${var.environment}-msk-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "MSK TLS from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-msk-sg"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# Storage
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "data" {
  bucket_prefix = "${var.project_name}-${var.environment}-data-"

  tags = {
    Name = "${var.project_name}-${var.environment}-data"
  }
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -----------------------------------------------------------------------------
# MSK Serverless (Kafka)
# -----------------------------------------------------------------------------
resource "aws_msk_serverless_cluster" "kafka" {
  cluster_name = "${var.project_name}-${var.environment}"

  vpc_config {
    subnet_ids         = aws_subnet.public[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

# -----------------------------------------------------------------------------
# IAM for EMR Serverless job execution
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_execution" {
  name = "${var.project_name}-${var.environment}-emr-exec"

  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json

  tags = {
    Name = "${var.project_name}-${var.environment}-emr-exec"
  }
}

data "aws_iam_policy_document" "emr_execution" {
  statement {
    sid    = "S3Access"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.data.arn,
      "${aws_s3_bucket.data.arn}/*"
    ]
  }

  statement {
    sid    = "MSKAccess"
    effect = "Allow"
    actions = [
      "kafka:DescribeCluster",
      "kafka:GetBootstrapBrokers",
      "kafka-cluster:*"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_execution" {
  name = "${var.project_name}-${var.environment}-emr-policy"
  role = aws_iam_role.emr_execution.id

  policy = data.aws_iam_policy_document.emr_execution.json
}

# -----------------------------------------------------------------------------
# EMR Serverless Application
# -----------------------------------------------------------------------------
resource "aws_emrserverless_application" "streaming" {
  name          = "${var.project_name}-${var.environment}"
  release_label = var.emr_release_label
  type          = "spark"

  network_configuration {
    subnet_ids         = aws_subnet.public[*].id
    security_group_ids = [aws_security_group.msk.id]
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-emr"
  }
}

# -----------------------------------------------------------------------------
# S3 object for job package (placeholder - uploaded by CI/CD)
# -----------------------------------------------------------------------------
resource "aws_s3_object" "job_placeholder" {
  bucket  = aws_s3_bucket.data.id
  key     = "jobs/streaming_job.py"
  content = file("${path.module}/../../src/pipeline/streaming_job.py")
  etag    = filemd5("${path.module}/../../src/pipeline/streaming_job.py")
}
