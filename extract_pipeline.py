import pandas as pd
import requests

from utils import extract_data, normalize_table, rename_columns


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

rename_columns(selected_columns, {
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
