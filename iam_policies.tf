# 1. IAM Role for Glue
resource "aws_iam_role" "glue_service_role" {
  name = "glue-service-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

# 2. Attach AWS Managed Policy (Basic Glue Permissions)
resource "aws_iam_role_policy_attachment" "glue_service_basic" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 3. Custom Policy: S3 Access
resource "aws_iam_policy" "glue_s3_access" {
  name        = "glue-s3-access"
  description = "Allow Glue to read/write to S3 buckets"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      Resource = [
        "arn:aws:s3:::prox-bronze-bucket",
        "arn:aws:s3:::prox-bronze-bucket/*",
        "arn:aws:s3:::prox-silver-bucket",
        "arn:aws:s3:::prox-silver-bucket/*",
        "arn:aws:s3:::prox-gold-bucket",
        "arn:aws:s3:::prox-gold-bucket/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

# 4. Custom Policy: CloudWatch Logs
resource "aws_iam_policy" "glue_logs" {
  name        = "glue-logs-policy"
  description = "Allow Glue to write logs to CloudWatch"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      Resource = "arn:aws:logs:*:*:*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_logs" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_logs.arn
}

# 5. Custom Policy: Glue Data Catalog (Optional)
resource "aws_iam_policy" "glue_catalog" {
  name        = "glue-catalog-policy"
  description = "Allow Glue to access Data Catalog"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable"
      ],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_catalog" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_catalog.arn
}

# 6. IAM Role for Redshift
resource "aws_iam_role" "redshift_role" {
  name = "prox-redshift-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "redshift.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_access" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role" "step_functions_role" {
  name = "StepFunctionsGlueOrchestrationRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_functions_permissions" {
  name = "StepFunctionsGlueAndSNSPermissions"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      # Glue job execution
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:GetJob"
        ]
        Resource = "*"
      },

      # Glue crawler control
      {
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlers"
        ]
        Resource = "*"
      },

      # SNS publish
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.notifications.arn
      }
    ]
  })
}
