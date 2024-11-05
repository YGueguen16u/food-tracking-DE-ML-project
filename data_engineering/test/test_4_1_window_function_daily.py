"""
Unit tests for functions performing data transformation and outlier detection.

These tests validate the creation of output files, the non-emptiness of processed data,
and ensure consistency in outlier detection across both Pandas and DuckDB implementations.

The tests include:
- Verifying the existence and non-emptiness of output files.
- Checking that outlier columns for each nutrient are present in both Pandas and DuckDB outputs.
- Comparing the consistency of outlier flags between Pandas and DuckDB outputs.
"""

import unittest
import os
import pandas as pd

FILE_PATH = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data"


class TestPandasWindowFunction(unittest.TestCase):
    """Test suite for validating the Pandas window function results."""

    @classmethod
    def setUpClass(cls):
        """Load the pandas output file once for all tests."""
        cls.df = pd.read_excel(f"{FILE_PATH}\\pandas_window_function_results_iqr.xlsx")

    def test_pandas_output_file_created(self):
        """Check if the Pandas output file exists in the specified location."""
        output_file = os.path.join(FILE_PATH, "pandas_window_function_results_iqr.xlsx")
        self.assertTrue(
            os.path.exists(output_file), "Pandas output file is not created"
        )

    def test_pandas_dataframe_not_empty(self):
        """Ensure that the Pandas DataFrame is not empty after processing."""
        self.assertFalse(self.df.empty, "Pandas DataFrame is empty")

    def test_pandas_outliers_categorization(self):
        """Verify each outlier column categorizes data according to IQR thresholds."""
        for nutrient in [
            "total_calories",
            "total_lipids",
            "total_carbs",
            "total_protein",
        ]:
            iqr_col = f"{nutrient}_outlier"
            self.assertIn(
                iqr_col, self.df.columns, f"{iqr_col} missing in Pandas DataFrame"
            )


class TestDuckDBWindowFunction(unittest.TestCase):
    """Test suite for validating the DuckDB window function results."""

    @classmethod
    def setUpClass(cls):
        """Load the DuckDB output file once for all tests."""
        cls.df = pd.read_excel(f"{FILE_PATH}\\duckdb_window_function_results_iqr.xlsx")

    def test_duckdb_output_file_created(self):
        """Check if the DuckDB output file exists in the specified location."""
        output_file = os.path.join(FILE_PATH, "duckdb_window_function_results_iqr.xlsx")
        self.assertTrue(
            os.path.exists(output_file), "DuckDB output file is not created"
        )

    def test_duckdb_dataframe_not_empty(self):
        """Ensure that the DuckDB DataFrame is not empty after processing."""
        self.assertFalse(self.df.empty, "DuckDB DataFrame is empty")

    def test_duckdb_outliers_categorization(self):
        """Verify each outlier column categorizes data according to IQR thresholds."""
        for nutrient in [
            "total_calories",
            "total_lipids",
            "total_carbs",
            "total_protein",
        ]:
            iqr_col = f"{nutrient}_outlier"
            self.assertIn(
                iqr_col, self.df.columns, f"{iqr_col} missing in DuckDB DataFrame"
            )


class TestOutliersComparison(unittest.TestCase):
    """Test suite to compare outliers detected by Pandas and DuckDB for consistency."""

    @classmethod
    def setUpClass(cls):
        """Load both Pandas and DuckDB output files and merge them for cross-validation."""
        pandas_df = pd.read_excel(
            f"{FILE_PATH}\\pandas_window_function_results_iqr.xlsx"
        )
        duckdb_df = pd.read_excel(
            f"{FILE_PATH}\\duckdb_window_function_results_iqr.xlsx"
        )
        cls.merged_df = pd.merge(
            pandas_df,
            duckdb_df,
            on=["user_id", "date"],
            suffixes=("_pandas", "_duckdb"),
        )

    def test_outliers_match(self):
        """Ensure that outliers flagged in Pandas match those in DuckDB for each nutrient."""
        for nutrient in ["calories", "lipids", "carbs", "protein"]:
            pandas_outlier_col = f"total_{nutrient}_outlier_pandas"
            duckdb_outlier_col = f"total_{nutrient}_outlier_duckdb"

            mismatched_rows = self.merged_df[
                self.merged_df[pandas_outlier_col] != self.merged_df[duckdb_outlier_col]
            ]
            if not mismatched_rows.empty:
                print(
                    f"Mismatched rows for {nutrient}:\n",
                    mismatched_rows[[pandas_outlier_col, duckdb_outlier_col]],
                )

            self.assertTrue(
                mismatched_rows.empty,
                f"Outliers for {nutrient} do not match between Pandas and DuckDB",
            )


if __name__ == "__main__":
    unittest.main()
