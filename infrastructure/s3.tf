
resource "aws_s3_bucket" "male_bucket" {
  bucket = "ayodeji-data-ingestion-bucket/random_profile/males"
}


# ayodeji-data-ingestion-bucket/random_profile/females


resource "aws_s3_bucket" "female_bucket" {
  bucket = "ayodeji-data-ingestion-bucket/random_profile/females"
}
