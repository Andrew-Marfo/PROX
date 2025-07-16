variable "region" {
  description = "The AWS region to deploy resources in"
  type        = string
}

variable "rds_host" {
  description = "RDS database host endpoint"
  type        = string
}

variable "rds_port" {
  description = "RDS database port"
  type        = string
}

variable "rds_db_name" {
  description = "RDS database name"
  type        = string
}

variable "rds_username" {
  description = "RDS database username"
  type        = string
}

variable "rds_password" {
  description = "RDS database password"
  type        = string
  sensitive   = true
}

variable "db_tables" {
  description = "Comma-separated list of tables to extract from RDS"
  type        = string
}

variable "redshift_cluster_identifier" {
  description = "Redshift cluster identifier"
  type        = string
}

variable "redshift_node_type" {
  description = "Redshift node type (e.g., ra3.xlplus, dc2.large)"
  type        = string
}

variable "redshift_master_username" {
  description = "Redshift master username"
  type        = string
}

variable "redshift_master_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "redshift_cluster_type" {
  description = "Redshift cluster type (single-node or multi-node)"
  type        = string
}

variable "redshift_subnet_ids" {
  description = "List of subnet IDs for Redshift subnet group"
  type        = list(string)
}

variable "redshift_vpc_id" {
  description = "VPC ID for Redshift security group"
  type        = string
}