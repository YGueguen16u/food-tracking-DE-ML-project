import unittest
import pandas as pd
import os
from data_engineering.Transform_data.windows_function import pandas_window_function, duckdb_window_function

class TestPandasWindowFunction(unittest.TestCase):

    FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data'

    def test_pandas_window_function_output(self):
        # Input and output file paths
        input_file = os.path.join(self.FILE_PATH, 'pandas_aggregation_results.xlsx')
        output_file = os.path.join(self.FILE_PATH, 'pandas_window_function_results.xlsx')

        # Debugging: Check if input file exists
        print(f"Full path to input file: {os.path.abspath(input_file)}")
        self.assertTrue(os.path.exists(input_file), f"Input file not found: {input_file}")

        # Run the pandas window function
        df = pandas_window_function()

        # Check if the output file was created
        print(f"Full path to output file: {os.path.abspath(output_file)}")
        self.assertTrue(os.path.exists(output_file), "Pandas output file not created.")

        # Load the generated file
        result_df = pd.read_excel(output_file)

        # Assert that the number of rows in the result matches the input
        input_df = pd.read_excel(input_file, sheet_name="User Daily Aggregation")
        self.assertEqual(len(result_df), len(input_df), "Row count mismatch between input and output.")

        # Assert that the expected columns exist
        expected_columns = [
            'date', 'user_id', 'day_of_week', 'total_calories', 'avg_last_4_calories',
            'calories_diff', 'calories_std_dev', 'calories_outlier',
            'total_lipids', 'avg_last_4_lipids', 'lipids_diff', 'lipids_std_dev', 'lipids_outlier',
            'total_carbs', 'avg_last_4_carbs', 'carbs_diff', 'carbs_std_dev', 'carbs_outlier',
            'total_protein', 'avg_last_4_protein', 'protein_diff', 'protein_std_dev', 'protein_outlier'
        ]
        self.assertTrue(all([col in result_df.columns for col in expected_columns]), "Missing expected columns in pandas results.")

        # Check for NaN handling in rolling averages
        self.assertFalse(result_df[['avg_last_4_calories', 'avg_last_4_lipids', 'avg_last_4_carbs', 'avg_last_4_protein']].isnull().any().any(), "NaN values found in rolling averages.")

        # Check if outliers are correctly marked as 0 or 1
        self.assertTrue(result_df['calories_outlier'].isin([0, 1]).all(), "Outliers in calories column are not marked correctly.")
        self.assertTrue(result_df['lipids_outlier'].isin([0, 1]).all(), "Outliers in lipids column are not marked correctly.")
        self.assertTrue(result_df['carbs_outlier'].isin([0, 1]).all(), "Outliers in carbs column are not marked correctly.")
        self.assertTrue(result_df['protein_outlier'].isin([0, 1]).all(), "Outliers in protein column are not marked correctly.")

        # Ensure numeric data types in calculated columns
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['calories_diff']), "Calories difference column is not numeric.")
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['calories_std_dev']), "Calories standard deviation column is not numeric.")


class TestDuckDBWindowFunction(unittest.TestCase):

    FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data'

    def test_duckdb_window_function_output(self):
        # Input and output file paths
        input_file = os.path.join(self.FILE_PATH, 'duckdb_aggregation_results.xlsx')
        output_file = os.path.join(self.FILE_PATH, 'duckdb_window_function_results.xlsx')

        # Debugging: Check if input file exists
        print(f"Full path to input file: {os.path.abspath(input_file)}")
        self.assertTrue(os.path.exists(input_file), f"Input file not found: {input_file}")

        # Run the DuckDB window function
        df = duckdb_window_function()

        # Check if the output file was created
        print(f"Full path to output file: {os.path.abspath(output_file)}")
        self.assertTrue(os.path.exists(output_file), "DuckDB output file not created.")

        # Load the generated file
        result_df = pd.read_excel(output_file)

        # Assert that the number of rows in the result matches the input
        input_df = pd.read_excel(input_file, sheet_name="User Daily Aggregation")
        self.assertEqual(len(result_df), len(input_df), "Row count mismatch between input and output.")

        # Assert that the expected columns exist
        expected_columns = [
            'date', 'user_id', 'day_of_week', 'total_calories', 'avg_last_4_calories',
            'calories_diff', 'calories_std_dev', 'calories_outlier',
            'total_lipids', 'avg_last_4_lipids', 'lipids_diff', 'lipids_std_dev', 'lipids_outlier',
            'total_carbs', 'avg_last_4_carbs', 'carbs_diff', 'carbs_std_dev', 'carbs_outlier',
            'total_protein', 'avg_last_4_protein', 'protein_diff', 'protein_std_dev', 'protein_outlier'
        ]
        self.assertTrue(all([col in result_df.columns for col in expected_columns]), "Missing expected columns in DuckDB results.")

        # Check for NaN handling in rolling averages
        self.assertFalse(result_df[['avg_last_4_calories', 'avg_last_4_lipids', 'avg_last_4_carbs', 'avg_last_4_protein']].isnull().any().any(), "NaN values found in rolling averages.")

        # Check if outliers are correctly marked as 0 or 1
        self.assertTrue(result_df['calories_outlier'].isin([0, 1]).all(), "Outliers in calories column are not marked correctly.")
        self.assertTrue(result_df['lipids_outlier'].isin([0, 1]).all(), "Outliers in lipids column are not marked correctly.")
        self.assertTrue(result_df['carbs_outlier'].isin([0, 1]).all(), "Outliers in carbs column are not marked correctly.")
        self.assertTrue(result_df['protein_outlier'].isin([0, 1]).all(), "Outliers in protein column are not marked correctly.")

        # Ensure numeric data types in calculated columns
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['calories_diff']), "Calories difference column is not numeric.")
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['calories_std_dev']), "Calories standard deviation column is not numeric.")


if __name__ == '__main__':
    unittest.main()
