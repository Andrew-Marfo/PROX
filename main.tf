provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project     = "Proximity-Based Service Provider Finder"
    }
  }
}

resource "aws_s3_bucket" "my_bucket" {
  bucket = "prox-bronze-layer"
}

