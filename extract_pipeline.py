import pandas as pd
import requests

from utils import extract_data, rename_columns


def extract_data(url):
    """
    this takes in the API's url
    and returns a JSON-parsed object
    """
    # check to see if the datatype is a string (url only)
    if type(url) != str:
          raise TypeError("Only strings are allowed")
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print('successful connection')
    except Exception as e:
        print('if not successfully connected')
        print(e)
    # parse the API response into json
    parsed_json = response.json()
    return parsed_json

data = response.json()
results = data['results']

normalized_result = pd.json_normalize(results)

# DATA TRANSFORMATION
# Convert to dataframe

normalized_frame = pd.DataFrame(normalized_result)


# DATA TRANSFORMATION: Select Columns

selected_columns = normalized_frame[[
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
