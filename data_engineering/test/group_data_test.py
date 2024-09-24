# Adjusted group_data_test.py to use FILE_PATH
import os
import unittest
from data_engineering.Transform_data.group_data import pandas_group_data, duckdb_group_data, compare_results

# Base file path
FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data'

class TestAggregationFunctions(unittest.TestCase):

    def test_pandas_group_data_real_files(self):
        file_path = os.path.join(FILE_PATH, 'combined_meal_data_filtered.xlsx')
        # Ensure the file exists
        self.assertTrue(os.path.exists(file_path))
        result = pandas_group_data()
        self.assertIn('daily_aggregation', result)

    def test_duckdb_group_data_real_files(self):
        file_path = os.path.join(FILE_PATH, 'combined_meal_data.xlsx')
        # Ensure the file exists
        self.assertTrue(os.path.exists(file_path))
        result = duckdb_group_data()
        self.assertIn('daily_aggregation', result)

if __name__ == '__main__':
    unittest.main()
