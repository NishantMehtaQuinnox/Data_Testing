import re
import cudf
import numpy as np
import os

class DataValidator:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def filename_matches_regex(self, filename, regex_pattern):
        return re.match(regex_pattern, filename) is not None

    def file_size_within_limit(self, filename, max_size_mb):
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        return file_size_mb <= max_size_mb

    def file_format_is_correct(self, filename, expected_format):
        return filename.lower().endswith(expected_format.lower())

    def record_count_matches(self, expected_count):
        return len(self.dataframe) == expected_count

    def column_data_types(self, expected_types):
        actual_types = self.dataframe.dtypes.to_dict()
        for column, expected_type in expected_types.items():
            if column not in actual_types or actual_types[column] != expected_type:
                return False
        return True

    def duplicates_exist(self, subset_columns=None):
        if subset_columns is None:
            return self.dataframe.shape[0] != self.dataframe.drop_duplicates().shape[0]
        else:
            return self.dataframe[subset_columns].shape[0] != self.dataframe[subset_columns].drop_duplicates().shape[0]

    def not_null_columns(self, subset_columns):
        if subset_columns:
            return self.dataframe[subset_columns].isnull().values.any() == False
        else:
            return self.dataframe.isnull().values.any() == False

    def field_length_within_range(self, column_name, min_length, max_length):
        field_lengths = self.dataframe[column_name].str.len()
        return ((field_lengths >= min_length) & (field_lengths <= max_length)).all()

    def custom_comparator(self, column_name, comparator_fn):
        return comparator_fn(self.dataframe[column_name])
    
    def generate_report(self, filename=None, regex_pattern=None, max_size_mb=None, 
                        expected_format=None, expected_count=None, expected_types=None, 
                        subset_columns=None, check_null_columns=None):

        report = {}

        if filename and regex_pattern is not None:
            report['filename_matches_regex'] = self.filename_matches_regex(filename, regex_pattern)

        if filename and max_size_mb is not None:
            report['file_size_within_limit'] = self.file_size_within_limit(filename, max_size_mb)

        if filename and expected_format is not None:
            report['file_format_is_correct'] = self.file_format_is_correct(filename, expected_format)

        if expected_count is not None:
            report['record_count_matches'] = self.record_count_matches(expected_count)

        if expected_types is not None:
            report['column_data_types'] = self.column_data_types(expected_types)

        if subset_columns is not None:
            report['duplicates_exist'] = self.duplicates_exist(subset_columns)
            
        if check_null_columns is not None:
            report['not_null_columns'] = self.not_null_columns(check_null_columns)

        return report
    
    
    def print_report(self, report):
        for test, result in report.items():
            print(f"{test}: {'PASS' if result else 'FAIL'}")
