terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
  backend "local" {
    path = "terraform.tfstate"
  }
}