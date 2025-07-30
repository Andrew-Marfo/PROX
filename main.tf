provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project = "Proximity-Based Service Provider Finder"
    }
  }
}

# Raw data bucket
resource "aws_s3_bucket" "bronze" {
  bucket        = "prox-bronze-bucket"
  force_destroy = true
}

# Curated data bucket
resource "aws_s3_bucket" "silver" {
  bucket        = "prox-silver-bucket"
  force_destroy = true
}

# Processed data bucket
resource "aws_s3_bucket" "gold" {
  bucket        = "prox-gold-bucket"
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
    path = "s3://${aws_s3_bucket.bronze.bucket}/snapservice/"
  }
  depends_on = [aws_s3_bucket.bronze]
}

# Silver Glue Crawler
resource "aws_glue_crawler" "silver_crawler" {
  name          = "silver-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  s3_target {
    path = "s3://${aws_s3_bucket.silver.bucket}/snapservice/"
  }
  depends_on = [aws_s3_bucket.silver]
}

# Gold Glue Crawler
resource "aws_glue_crawler" "gold_crawler" {
  name          = "gold-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  s3_target {
    path = "s3://${aws_s3_bucket.gold.bucket}/star_schema/"
  }
  depends_on = [aws_s3_bucket.gold]
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
  name              = "bronze-ingestion-job"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/bronze_ingestion_script.py"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--TempDir"          = "s3://${aws_s3_bucket.bronze.bucket}/temp/"
    "--enable-metrics"   = ""
    "--enable-spark-ui"  = "true"
    "--JOB_NAME"         = "bronze-ingestion-job"
    "--RDS_HOST"         = var.rds_host
    "--RDS_PORT"         = var.rds_port
    "--RDS_DB_NAME"      = var.rds_db_name
    "--RDS_USERNAME"     = var.rds_username
    "--RDS_PASSWORD"     = var.rds_password
    "--S3_OUTPUT_BUCKET" = aws_s3_bucket.bronze.bucket
    "--DB_TABLES"        = var.db_tables
  }

  depends_on = [aws_s3_object.bronze_ingestion_script]
}

# Glue job for transformations
resource "aws_glue_job" "transformations_job" {
  name              = "transformations-job"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/transformations_script.py"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--TempDir"          = "s3://${aws_s3_bucket.silver.bucket}/temp/"
    "--enable-metrics"   = ""
    "--enable-spark-ui"  = "true"
    "--JOB_NAME"         = "transformations-job"
    "--SOURCE_DATABASE"  = aws_glue_catalog_database.lakehouse.name
    "--S3_OUTPUT_BUCKET" = aws_s3_bucket.silver.bucket
    "--DB_TABLES"        = var.db_tables
  }

  depends_on = [aws_s3_object.transformations_script]
}

# Sns Topic for notifications
resource "aws_sns_topic" "notifications" {
  name = "prox-notifications"
}

# gold_data_curation_script location
resource "aws_s3_object" "gold_data_curation_script" {
  bucket = aws_s3_bucket.gold.bucket
  key    = "scripts/gold_data_curation_script.py"
  source = "${path.module}/glue_scripts/gold_data_curation_script.py"
  etag   = filemd5("${path.module}/glue_scripts/gold_data_curation_script.py")
}

# Glue job for gold data curation
resource "aws_glue_job" "gold_data_curation_job" {
  name              = "gold-data-curation-job"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.gold.bucket}/scripts/gold_data_curation_script.py"
  }

  default_arguments = {
    "--job-language"     = "python"
    "--TempDir"          = "s3://${aws_s3_bucket.gold.bucket}/temp/"
    "--enable-metrics"   = ""
    "--enable-spark-ui"  = "true"
    "--JOB_NAME"         = "gold-data-curation-job"
    "--SOURCE_DATABASE"  = aws_glue_catalog_database.lakehouse.name
    "--S3_OUTPUT_BUCKET" = aws_s3_bucket.gold.bucket
  }

  depends_on = [aws_s3_object.gold_data_curation_script]
}

# --- VPC ---
resource "aws_vpc" "redshift_vpc" {
  cidr_block           = var.vpc_cidr_block
  tags                 = { Name = "redshift-vpc" }
  enable_dns_support   = true
  enable_dns_hostnames = true
}

# Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.redshift_vpc.id
}

# Route Table
resource "aws_route_table" "main" {
  vpc_id = aws_vpc.redshift_vpc.id
  route {
    cidr_block = var.route_table_cidr_block
    gateway_id = aws_internet_gateway.igw.id
  }
}


# --- Subnets ---
# First subnet
resource "aws_subnet" "redshift_subnet_a" {
  vpc_id                  = aws_vpc.redshift_vpc.id
  cidr_block              = var.redshift_subnet_cidr_a
  availability_zone       = var.availability_zone_a
  map_public_ip_on_launch = true
}

#Second subnet for redshift
resource "aws_subnet" "redshift_subnet_b" {
  vpc_id                  = aws_vpc.redshift_vpc.id
  cidr_block              = var.redshift_subnet_cidr_b
  availability_zone       = var.availability_zone_b
  map_public_ip_on_launch = true
}

# Route Table Association for Subnets
resource "aws_route_table_association" "redshift_subnet_a" {
  subnet_id      = aws_subnet.redshift_subnet_a.id
  route_table_id = aws_route_table.main.id
}

resource "aws_route_table_association" "redshift_subnet_b" {
  subnet_id      = aws_subnet.redshift_subnet_b.id
  route_table_id = aws_route_table.main.id
}


# Security group for Redshift
resource "aws_security_group" "redshift_sg" {
  name        = "prox-redshift-sg"
  description = "Security group for Redshift cluster"
  vpc_id      = aws_vpc.redshift_vpc.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Restrict as needed for security
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  depends_on = [
    aws_vpc.redshift_vpc
  ]
}

# Subnet group for Redshift
resource "aws_redshift_subnet_group" "redshift_subnet_group" {
  name        = "prox-redshift-subnet-group"
  description = "Subnet group for Redshift cluster"
  subnet_ids = [
    aws_subnet.redshift_subnet_a.id,
    aws_subnet.redshift_subnet_b.id
  ]

  depends_on = [
    aws_subnet.redshift_subnet_a,
    aws_subnet.redshift_subnet_b
  ]
}

# Redshift Cluster
resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier        = var.redshift_cluster_identifier
  database_name             = var.redshift_db_name
  master_username           = var.redshift_master_username
  master_password           = var.redshift_master_password
  node_type                 = var.redshift_node_type
  cluster_type              = var.redshift_cluster_type
  number_of_nodes           = var.redshift_number_of_nodes
  skip_final_snapshot       = true
  publicly_accessible       = true
  iam_roles                 = [aws_iam_role.redshift_role.arn]
  vpc_security_group_ids    = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.name

  tags = {
    Name = "Proximity Redshift Cluster"
  }

  depends_on = [
    aws_redshift_subnet_group.redshift_subnet_group,
    aws_security_group.redshift_sg,
    aws_iam_role.redshift_role
  ]
}

# Upload s3_to_redshift_script.py to S3
resource "aws_s3_object" "s3_to_redshift_script" {
  bucket = aws_s3_bucket.gold.bucket
  key    = "scripts/s3_to_redshift_script.py"
  source = "${path.module}/glue_scripts/s3_to_redshift_script.py"
  etag   = filemd5("${path.module}/glue_scripts/s3_to_redshift_script.py")
}

# Glue job to load data from S3 to Redshift
resource "aws_glue_job" "s3_to_redshift_job" {
  name              = "s3-to-redshift-job"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.gold.bucket}/scripts/s3_to_redshift_script.py"
  }

  default_arguments = {
    "--job-language"      = "python"
    "--TempDir"           = "s3://${aws_s3_bucket.gold.bucket}/temp/"
    "--enable-metrics"    = ""
    "--enable-spark-ui"   = "true"
    "--JOB_NAME"          = "s3-to-redshift-job"
    "--REDSHIFT_USER"     = var.redshift_master_username
    "--REDSHIFT_PASSWORD" = var.redshift_master_password
    "--REDSHIFT_DB"       = var.redshift_db_name
    "--REDSHIFT_HOST"     = aws_redshift_cluster.redshift_cluster.endpoint
    "--REDSHIFT_PORT"     = aws_redshift_cluster.redshift_cluster.port
    "--REDSHIFT_SCHEMA"   = "public"
    "--S3_OUTPUT_BUCKET"  = aws_s3_bucket.gold.bucket
    "--IAM_ROLE"          = aws_iam_role.redshift_role.arn
  }

  depends_on = [
    aws_s3_object.s3_to_redshift_script,
    aws_redshift_cluster.redshift_cluster,
    aws_iam_role.redshift_role
  ]
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.bucket
}