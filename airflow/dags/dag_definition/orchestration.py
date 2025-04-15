

from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from random_profiles.pipeline_utils import (extract_data, extract_female,
                                            extract_male, female_s3_load,
                                            male_s3_load, normalize_table,
                                            rename_columns)

from airflow import DAG

with DAG(
    dag_id="data_ingestion",
    start_date=datetime(2024, 11, 22),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    get_profiles = PythonOperator(
        task_id="getting_profiles",
        python_callable=extract_data
    )

    normalization_and_column_selection = PythonOperator(
        task_id="normalize_and_select_columns",
        python_callable=normalize_table
    )

    renaming_columns = PythonOperator(
        task_id="renaming_selected_columns",
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

    get_profiles >> normalization_and_column_selection >> renaming_columns >> \
        [extract_male_gender, extract_female_gender]
    extract_male_gender >> for_male_s3_load
    extract_female_gender >> for_female_s3_load
