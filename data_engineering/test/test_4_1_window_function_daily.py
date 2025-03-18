"""
Unit tests for calculating rolling averages of meal data using both Pandas and DuckDB.
These tests validate that the rolling averages for the last 4 same weekdays
(e.g., last 4 Thursdays) are computed correctly and the results match expectations.
"""

import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from data_engineering.Transform_data.transform_4_1_window_function_daily import (
    DuckDBWindowFunction,
    PandasWindowFunction,
)


class TestWindowFunction(unittest.TestCase):
    """
    Unit tests for Pandas and DuckDB window functions calculating rolling averages.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up test data for both Pandas and DuckDB implementations.
        """
        # Test input DataFrame
        cls.test_data = pd.DataFrame(
            {
                "date": [
                    "2024-01-04",
                    "2024-01-11",
                    "2024-01-18",
                    "2024-01-25",
                    "2024-02-01",
                    "2024-02-08",
                    "2024-02-15",
                    "2024-02-22",
                ],
                "total_calories": [2000, 2200, 2040, 2360, 2480, 2300, 2320, 3140],
                "total_lipids": [20, 22, 24, 26, 28, 30, 32, 34],
                "total_carbs": [100, 110, 120, 130, 140, 150, 160, 170],
                "total_protein": [50, 55, 60, 65, 70, 75, 80, 85],
            }
        )
        cls.test_data["date"] = pd.to_datetime(
            cls.test_data["date"]
        )  # Ensure datetime format

        # Expected DataFrame after rolling averages calculation
        cls.expected_output = cls.test_data.copy()
        cls.expected_output["weekday"] = cls.expected_output["date"].dt.day_name()

        # Manually calculated rolling averages (last 4 same weekdays)
        cls.expected_output["rolling_avg_total_calories"] = [
            2000.0,
            2100.0,
            2080.0,
            2150.0,
            2270.0,
            2295.0,
            2365.0,
            2560.0,
        ]
        cls.expected_output["rolling_avg_total_lipids"] = [
            20.0,
            21.0,
            22.0,
            23.0,
            25.0,
            27.0,
            29.0,
            31.0,
        ]
        cls.expected_output["rolling_avg_total_carbs"] = [
            100.0,
            105.0,
            110.0,
            115.0,
            125.0,
            135.0,
            145.0,
            155.0,
        ]
        cls.expected_output["rolling_avg_total_protein"] = [
            50.0,
            52.5,
            55.0,
            57.5,
            62.5,
            67.5,
            72.5,
            77.5,
        ]

    def setUp(self):
        """
        Initializes instances for testing Pandas and DuckDB implementations.
        """
        self.agg_pandas = PandasWindowFunction(dataframe=self.test_data)
        self.agg_duckdb = DuckDBWindowFunction(dataframe=self.test_data)

    def test_pandas_window_function(self):
        """
        Test the rolling averages calculation with the Pandas implementation.
        """
        result = self.agg_pandas.compute_daily_window_pandas().round(4)
        # Ensure all columns match the expected output
        assert_frame_equal(
            result[
                [
                    "date",
                    "total_calories",
                    "total_lipids",
                    "total_carbs",
                    "total_protein",
                    "weekday",
                    "rolling_avg_total_calories",
                    "rolling_avg_total_lipids",
                    "rolling_avg_total_carbs",
                    "rolling_avg_total_protein",
                ]
            ],
            self.expected_output,
            check_like=True,  # Ignore column order
        )
        print("Pandas test passed.")

    def test_duckdb_window_function(self):
        """
        Test the rolling averages calculation with the DuckDB implementation.
        """
        result = self.agg_duckdb.compute_daily_window_duckdb().round(4)
        # Ensure all columns match the expected output
        assert_frame_equal(
            result[
                [
                    "date",
                    "total_calories",
                    "total_lipids",
                    "total_carbs",
                    "total_protein",
                    "weekday",
                    "rolling_avg_total_calories",
                    "rolling_avg_total_lipids",
                    "rolling_avg_total_carbs",
                    "rolling_avg_total_protein",
                ]
            ],
            self.expected_output,
            check_like=True,  # Ignore column order
        )
        print("DuckDB test passed.")


if __name__ == "__main__":
    unittest.main()
