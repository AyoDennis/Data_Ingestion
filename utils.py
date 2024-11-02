import pandas as pd
import requests


def extract_data(url):
    """
    this takes in the API's url
    and returns a JSON-parsed object
    """
    # check to see if the datatype is a string (url only)
    if type(url) is not str:
        raise TypeError("Only strings are allowed")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print('The connection was successful')
            # parse the API response into json
            parsed_json = response.json()
            return parsed_json
        else:
            print(f"The connection was unsuccessful{response.status_code}")
    except Exception as e:
        print(f'Unsuccessful connection, {e}')


def normalize_table(parsed_json):
    """
    this takes in the parsed JSON, filters 'results'
    and returns a normalized dataframe
    """
    # filter the index of 'results'
    results = parsed_json['results']
    # normalizes everything in the 'results' object
    normalized_result = pd.json_normalize(results)
    # converts normalized result into a DataFrame
    normalized_df = pd.DataFrame(normalized_result)
    return normalized_df


def rename_columns(df, new_names):
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
    renamed_df = df.rename(columns=new_names)
    return renamed_df


def extract_male(renamed_df):
    """
    This function filters males from the gender column
    """
    males = renamed_df[renamed_df.gender == 'male']
    return males


def extract_female(renamed_df):
    """
    This function filters females from the gender column.
    """
    females = renamed_df[renamed_df.gender == 'female']
    return females
