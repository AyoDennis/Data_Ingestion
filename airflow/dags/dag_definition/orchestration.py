import airflow
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.python import PythonOperator
from random_profiles.utils import (extract_data, extract_female, extract_male,
                   file_conversion_and_s3_load, normalize_table,
                   rename_columns)

with DAG(
    dag_id="data_ingestion",
    start_date=datetime(2024,11,15),
    schedule_interval=None,
    catchup=False
) as dag:
    get