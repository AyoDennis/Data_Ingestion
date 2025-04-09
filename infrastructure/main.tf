# Terraform Remote State
terraform {
  backend "s3" {
    bucket = "ayodeji-state-bucket-data-ingestion-1"
    key    = "key/terraform.tfstate"
    region = "eu-central-1"
  }
}
