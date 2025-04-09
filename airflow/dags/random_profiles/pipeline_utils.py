import logging

import awswrangler as wr
import boto3
import pandas as pd
import requests
from airflow.models import Variable

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


def aws_session():
    session = boto3.Session(
                    aws_access_key_id=Variable.get('access_key'),
                    aws_secret_access_key=Variable.get('secret_key'),
                    region_name="eu-central-1"
    )
    return session


def boto3_client(aws_service):

    client = boto3.client(aws_service,
                          aws_access_key_id=Variable.get('access_key'),
                          aws_secret_access_key=Variable.get('secret_key'),
                          region_name="eu-central-1")

    return client


def extract_data():
    """
    this takes in the API's url
    and returns a JSON-parsed object
    """
    url = 'https://randomuser.me/api/?results=1000'
    if type(url) is not str:
        raise TypeError("Only strings are allowed")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logging.info('The connection was successful')
            parsed_json = response.json()
            return parsed_json
        else:
            logging.info(f"The connection unsuccessful{response.status_code}")
    except Exception as e:
        print(f'Unsuccessful connection, {e}')


logging.info("finished making API request and parsing JSON object")


def normalize_table():
    """
    this takes in the parsed JSON, filters 'results'
    and returns a normalized dataframe,
    selects needed columns
    """
    parsed_json = extract_data()
    results = parsed_json['results']
    normalized_result = pd.json_normalize(results)
    selected_columns = normalized_result[[
        'gender',
        'name.title',
        'name.first',
        'name.last',
        'cell',
        'email',
        'location.street.number',
        'location.street.name',
        'location.city',
        'location.country',
        'login.username',
        'login.password',
        'dob.date',
        'dob.age'
    ]]
    logging.info("finished normalization and column selection")
    return selected_columns


def rename_columns():
    """
    This function is for renaming columns in a pandas DataFrame.
    Args:
        df: The pandas DataFrame to rename columns in.
        new_names: A dictionary mapping old to new column names in this format.
        new_names =
        {
            'old_column_1': 'new_column_1',
            'old_column_2': 'new_column_2',
        # ...
        }
    Returns:
        The DataFrame with renamed columns.
     """
    renamed_dic = {
        'gender': 'gender',
        'name.title': 'title',
        'name.first': 'first_name',
        'name.last': 'last_name',
        'cell': 'phone',
        'location.street.number': 'street_number',
        'location.street.name': 'street_name',
        'location.city': 'city',
        'location.country': 'country',
        'login.username': 'username',
        'login.password': 'password',
        'dob.date': 'dob',
        'dob.age': 'age'
    }
    df = normalize_table()
    renamed_df = df.rename(columns=renamed_dic)
    logging.info("finished renaming columns")
    return renamed_df


def extract_male():
    """
    This function filters males from the gender column
    """
    test = rename_columns()
    males = test[test["gender"] == "male"]
    logging.info("created male table")
    return males


def extract_female():
    """
    This function filters males from the gender column
    """
    test = rename_columns()
    females = test[test["gender"] == "female"]
    logging.info("created female table")
    return females


def male_s3_load():
    """
    Converts a DataFrame to Parquet and loads it to S3.
    """
    s3_path = "s3://ayodeji-data-ingestion-bucket/random_profile/males"
    logging.info("s3 object initiated")
    wr.s3.to_parquet(
        df=extract_male(),
        path=s3_path,
        mode="overwrite",
        boto3_session=aws_session(),
        dataset=True
    )
    logging.info("parquet conversion successful")
    return "Data successfully written to S3"


def female_s3_load():
    """
    Converts a DataFrame to Parquet and loads it to S3.
    """
    s3_path = "s3://ayodeji-data-ingestion-bucket/random_profile/females"
    logging.info("s3 object initiated")
    wr.s3.to_parquet(
        df=extract_female(),
        path=s3_path,
        mode="overwrite",
        boto3_session=aws_session(),
        dataset=True
    )
    logging.info("parquet conversion successful")
    return "Data successfully written to S3"
