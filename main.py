from data_loader import JsonToGDFLoader
from validator import DataValidator

# Define your transformations list as follows:
transformations = [
    {
        'from_column_name': 'original_column1',
        'to_column_name': 'new_column1',
        'type_update': 'int64',
        'transformation_function': lambda x: x * 2
    },
    {
        'from_column_name': 'original_column2',
        'to_column_name': 'new_column2',
        'type_update': None,
        'transformation_function': lambda x: x.strip() if isinstance(x, str) else x
    }
    # Add as many dictionaries as needed for transformations
]

folder_url = 'data_testing/7bc4e498-e698-11ee-9eb5-025f58bc16f6'
loader = JsonToGDFLoader(folder_url)
df = loader.load()

tdf = loader.transform_gdf(df,transformations)

# Assuming `df` is your cuDF DataFrame and 'data.json' is the file you're validating
validator = DataValidator(df)

# Define your validations
filename_regex = r'^data_\\d{4}.json$'
file_size_limit_mb = 10
file_format = '.json'
expected_record_count = 7740001
expected_data_types = {
    'name': 'object', 
    'age': 'int64',
    # Other columns and their expected types...
}
expected_not_null_columns = ['name', 'age']
expected_field_length = {
    'name': (1, 50)
}

# Perform validations
# assert validator.filename_matches_regex('data.json', filename_regex)
# assert validator.file_size_within_limit('data.json', file_size_limit_mb)
# assert validator.file_format_is_correct('data.json', file_format)
assert validator.record_count_matches(expected_record_count)
assert validator.column_data_types(expected_data_types)
assert not validator.duplicates_exist()
assert validator.not_null_columns(expected_not_null_columns)
assert validator.field_length_within_range('name', *expected_field_length['name'])
# For custom comparators, provide the lambda or function
# assert validator.custom_comparator('age', lambda ages: (ages > 0).all())

# If any assertion fails, it will raise an AssertionError.
