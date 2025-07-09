provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project     = "Proximity-Based Service Provider Finder"
    }
  }
}

# Raw data bucket
resource "aws_s3_bucket" "bronze" {
  bucket = "prox-bronze-bucket"
  force_destroy = true
}

# Curated data bucket
resource "aws_s3_bucket" "silver" {
  bucket = "prox-silver-bucket"
  force_destroy = true
}

# Processed data bucket
resource "aws_s3_bucket" "gold" {
  bucket = "prox-gold-bucket"
  force_destroy = true
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "lakehouse" {
  name = "prox_lakehouse"
}

# Bronze Glue Crawler
resource "aws_glue_crawler" "bronze_crawler" {
  name          = "bronze-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  s3_target {
    path = "s3://${aws_s3_bucket.bronze.bucket}/prox/"
  }
  depends_on = [aws_s3_bucket.bronze]
}

# Silver Glue Crawler
resource "aws_glue_crawler" "silver_crawler" {
  name          = "silver-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  s3_target {
    path = "s3://${aws_s3_bucket.silver.bucket}/prox/"
  }
  depends_on = [aws_s3_bucket.silver]
}

# bronze_ingestion script location
resource "aws_s3_object" "bronze_ingestion_script" {
  bucket = aws_s3_bucket.bronze.bucket
  key    = "scripts/bronze_ingestion_script.py"
  source = "${path.module}/glue_scripts/bronze_ingestion_script.py" # Ensure this file exists in your Terraform directory
  etag   = filemd5("${path.module}/glue_scripts/bronze_ingestion_script.py")
}

# transformations_script location
resource "aws_s3_object" "transformations_script" {
  bucket = aws_s3_bucket.bronze.bucket
  key    = "scripts/transformations_script.py"
  source = "${path.module}/glue_scripts/transformations_script.py"
  etag   = filemd5("${path.module}/glue_scripts/transformations_script.py")
}

# Glue job to load data from rds to bronze
resource "aws_glue_job" "bronze_ingestion_job" {
  name            = "bronze-ingestion-job"
  role_arn        = aws_iam_role.glue_service_role.arn
  glue_version    = "4.0"
  worker_type     = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/bronze_ingestion_script.py"
  }

  default_arguments = {
    "--job-language"      = "python"
    "--TempDir"           = "s3://${aws_s3_bucket.bronze.bucket}/temp/"
    "--enable-metrics"    = ""
    "--enable-spark-ui"   = "true"
    "--JOB_NAME"          = "bronze-ingestion-job"
    "--RDS_HOST"          = var.rds_host
    "--RDS_PORT"          = var.rds_port
    "--RDS_DB_NAME"       = var.rds_db_name
    "--RDS_USERNAME"      = var.rds_username
    "--RDS_PASSWORD"      = var.rds_password
    "--S3_OUTPUT_BUCKET"  = aws_s3_bucket.bronze.bucket
    "--TABLES_TO_EXTRACT" = var.db_tables
  }

  depends_on = [aws_s3_object.bronze_ingestion_script]
}

# Glue job for transformations
resource "aws_glue_job" "transformations_job" {
  name            = "transformations-job"
  role_arn        = aws_iam_role.glue_service_role.arn
  glue_version    = "4.0"
  worker_type     = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/transformations_script.py"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--TempDir"             = "s3://${aws_s3_bucket.silver.bucket}/temp/"
    "--enable-metrics"      = ""
    "--enable-spark-ui"     = "true"
    "--JOB_NAME"            = "transformations-job"
    "--SOURCE_DATABASE"     = aws_glue_catalog_database.lakehouse.name
    "--S3_OUTPUT_BUCKET"    = aws_s3_bucket.silver.bucket
    "--DB_TABLES"           = var.db_tables
  }

  depends_on = [aws_s3_object.transformations_script]
}

# Sns Topic for notifications
resource "aws_sns_topic" "notifications" {
  name = "prox-notifications"
}

