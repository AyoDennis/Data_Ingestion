import pandas as pd
import requests

from utils import (extract_data, extract_female, extract_male, normalize_table,
                   rename_columns)




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

# helper functions
url = "https://randomuser.me/api/"

parsed_json = extract_data(url)


normalized_df = normalize_table(parsed_json)

# DATA TRANSFORMATION: Select Columns

selected_columns = normalized_df[[
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


# DATA TRANSFORMATION: Rename Columns

renamed_df = rename_columns(selected_columns, {
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
    })

# DATA TRANSFORMATION: Split tables based on gender

male_table = extract_male(renamed_df)

female_table = extract_female(renamed_df)
