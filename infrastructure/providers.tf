terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-central-1"
}

# # Terraform Remote State

# terraform {
#   backend "s3" {
#     bucket = "ayodeji-state-bucket-data-ingestion"
#     key    = "key/terraform.tfstate"
#     region = "eu-central-1"
#   }
# }

# resource "aws_s3_bucket_versioning" "backend_versioning" {
#   bucket = aws_s3_bucket.ayodeji-state-bucket-data-ingestion.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }