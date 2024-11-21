

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from random_profiles.pipeline_utils import (extract_data, extract_female,
                                            extract_male, female_s3_load,
                                            male_s3_load, rename_columns)

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

    renaming_columns = PythonOperator(
        task_id="renaming_columns",
        python_callable=rename_columns
    )

    extract_male_gender = PythonOperator(
        task_id="extract_male_gender",
        python_callable=extract_male
    )

    extract_female_gender = PythonOperator(
        task_id="extract_female_gender",
        python_callable=extract_female
    )

    for_male_s3_load = PythonOperator(
        task_id="male_parquet_s3_load",
        python_callable=male_s3_load
    )

    for_female_s3_load = PythonOperator(
        task_id="female_parquet_s3_load",
        python_callable=female_s3_load
    )

    get_profiles >> renaming_columns >> \
        [extract_male_gender, extract_female_gender]
    extract_male_gender >> for_male_s3_load
    extract_female_gender >> for_female_s3_load

