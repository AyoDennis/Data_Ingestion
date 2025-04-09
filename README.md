# Data_Ingestion
## ETL Data Ingestion Project with Random Profiles API, AWS S3, and Airflow
Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline that:

* Extracts random user profile data from the Random User Generator API
* Transforms the data into a structured format
* Loads the processed data into an AWS S3 bucket
* Orchestrates the entire process using Apache Airflow

The infrastructure is provisioned using Terraform, and the application runs in Docker containers orchestrated with Docker Compose.

## Features

`Data Extraction:` Fetches random user profiles from the public API
`Data Transformation:` Processes and structures the raw API data
`Data Loading:` Stores the transformed data in AWS S3 using AWS Data Wrangler
`Orchestration:` Managed by Apache Airflow for scheduling and monitoring
`Infrastructure as Code: AWS resources provisioned with Terraform
`Containerized:` Dockerized application with Compose for local development

## Technology Stack

`Data Processing:` Python, AWS Data Wrangler (awswrangler)
`Orchestration:` Apache Airflow
`Cloud Storage:` AWS S3
`Infrastructure:` Terraform
`Containerization:` Docker, Docker Compose

## Infrastructure

* AWS S3 bucket for data storage
* Remote statefile storage for Terraform
* IAM user with appropriate permissions for Airflow

## Setup Instructions

### Prerequisites

* Docker and Docker Compose installed
* AWS account with appropriate permissions
* Terraform installed (for infrastructure provisioning)
* Installation

### Provision AWS Resources with Terraform:
`bash`

cd terraform/
terraform init
terraform plan
terraform apply

### Build and Start Containers:
`bash`

docker-compose build
docker-compose up -d

### Access Airflow UI:

Open http://localhost:8080 in your browser (default credentials: airflow/airflow)


## Usage

Configure the Airflow connection to AWS in the Airflow UI
Trigger the ETL DAG manually or wait for scheduled execution
Monitor pipeline execution in the Airflow UI
Verify data in the designated S3 bucket
Configuration

Environment variables can be set in the docker-compose.yml file or through an .env file:

AWS_ACCESS_KEY_ID: AWS access key
AWS_SECRET_ACCESS_KEY: AWS secret key
AWS_DEFAULT_REGION: AWS region
S3_BUCKET_NAME: Target S3 bucket name
License

This project is licensed under the MIT License - see the LICENSE file for details.

Acknowledgments

Random User Generator API for providing the test data
Apache Airflow, AWS, and Terraform communities for their excellent documentation