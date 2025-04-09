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
