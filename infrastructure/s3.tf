# resource "aws_s3_bucket" "data_ingestion" {
#   bucket = "ayodeji-data-ingestion-bucket/random_profile"
# }


resource "aws_s3_bucket" "data_ingestion" {
  bucket = "ayodeji-data-ingestion-bucket"
}
