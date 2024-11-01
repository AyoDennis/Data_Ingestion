import requests
import pandas as pd
from rename_dataframe import rename_columns

response = requests.get('https://randomuser.me/api/')

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
