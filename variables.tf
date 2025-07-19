variable "region" {
  description = "The AWS region to deploy resources in"
  type        = string
}

# rds variables
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

# Redshift variables
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

variable "redshift_db_name" {
  description = "Redshift database name"
  type        = string
}

variable "redshift_number_of_nodes" {
  description = "Number of nodes in the Redshift cluster"
  type        = number
  
}

variable "redshift_cluster_type" {
  description = "Redshift cluster type (single-node or multi-node)"
  type        = string
}

# vpc and subnet variables
variable "redshift_subnet_cidr_a" {
  description = "CIDR block for Redshift subnet in Availability Zone A"
  type        = string
}

variable "vpc_cidr_block" {
  description = "CIDR block for the VPC"
  type        = string
}

variable "route_table_cidr_block" {
  description = "CIDR block for the route table"
  type        = string
}

variable "availability_zone_a" {
  description = "Availability Zone A for resources"
  type        = string
}

variable "availability_zone_b" {
  description = "Availability Zone B for resources"
  type        = string
}

variable "redshift_subnet_cidr_b" {
  description = "CIDR block for Redshift subnet in Availability Zone B"
  type        = string
}