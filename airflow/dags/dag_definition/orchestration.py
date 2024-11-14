from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from random_profiles.pipeline_utils import (extract_data, extract_female,
                                            extract_male,
                                            file_conversion_and_s3_load,
                                            normalize_table, rename_columns)

with DAG(
    dag_id="data_ingestion",
    start_date=datetime(2024, 11, 15),
    schedule_interval=None,
    catchup=False
) as dag:
    get_profiles = PythonOperator(
        task_id="getting_profiles",
        python_callable=extract_data
    )

    normalize_tables = PythonOperator(
        task_id="normalizing_table",
        python_callable=normalize_table
    )

    renaming_columns = PythonOperator(
        task_id="renaming columns",
        python_callable=rename_columns
    )

    extract_male_gender = PythonOperator(
        task_id="extract_male_gender",
        python_callable=extract_male
    )

    extract_female_gender = PythonOperator(
        task_id="extract_male_gender",
        python_callable=extract_female
    )

    parquet_conversion_and_s3_load = PythonOperator(
        task_id="file_conversion_and_s3_load",
        python_callable=file_conversion_and_s3_load
    )

    get_profiles >> normalize_tables >> \
        renaming_columns >> extract_male_gender >> extract_female_gender >> \
        parquet_conversion_and_s3_load
