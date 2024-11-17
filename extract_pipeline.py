from initial_utils import (extract_data, extract_female, extract_male,
                   file_conversion_and_s3_load, normalize_table,
                   rename_columns)

url = "https://randomuser.me/api/?results=20"

parsed_json = extract_data(url)


normalized_df = normalize_table(parsed_json)


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


male_table = extract_male(renamed_df)

female_table = extract_female(renamed_df)

send_male = file_conversion_and_s3_load(male_table)

send_female = file_conversion_and_s3_load(female_table)
