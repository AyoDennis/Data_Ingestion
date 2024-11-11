import logging

import awswrangler as wr
import boto3
import pandas as pd
import requests

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

client = boto3.client('s3')
logging.info("s3 client initiated")
client.create_bucket(Bucket='ayodeji-data-ingestion-bucket')


def extract_data(url):
    """
    this takes in the API's url
    and returns a JSON-parsed object
    """
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


def normalize_table(parsed_json):
    """
    this takes in the parsed JSON, filters 'results'
    and returns a normalized dataframe
    """
    results = parsed_json['results']
    normalized_result = pd.json_normalize(results)
    normalized_df = pd.DataFrame(normalized_result)
    logging.info("finished dataframe conversion and normalization")
    return normalized_df


def rename_columns(normalized_df, new_names):
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
    renamed_df = normalized_df.rename(columns=new_names)
    logging.info("finished renaming columns")
    return renamed_df


def extract_male(renamed_df):
    """
    This function filters males from the gender column
    """
    males = renamed_df[renamed_df.gender == 'male']
    logging.info("created male table")
    return males


def extract_female(renamed_df):
    """
    This function filters females from the gender column.
    """
    females = renamed_df[renamed_df.gender == 'female']
    logging.info("created female table")
    return females


def extract_random_profile_to_s3(df):
    """
    Converts a DataFrame to Parquet and loads it to S3.
    """
    s3_path = "s3://ayodeji-data-ingestion-bucket/random_profile/"
    logging.info("s3 object initiated")
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        mode="append",
        dataset=True
    )
    logging.info("parquet conversion successful")
    return "Data successfully written to S3"
