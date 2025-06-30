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

variable "tables_to_extract" {
  description = "Comma-separated list of tables to extract from RDS"
  type        = string
}