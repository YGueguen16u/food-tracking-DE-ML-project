"""
Unit tests for calculating percentage changes in nutrient data using both Pandas and DuckDB.
These tests validate that percentage changes relative to rolling averages are computed correctly
and the results match expectations.
"""

import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from data_engineering.Transform_data.transform_5_1_percentage_change_daily import (
    DuckDBPercentageChange, PandasPercentageChange)


class TestPercentageChange(unittest.TestCase):
    """
    Unit tests for Pandas and DuckDB implementations calculating percentage changes.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up test data for both Pandas and DuckDB implementations.
        """
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
                "rolling_avg_total_calories": [
                    2000.0,
                    2100.0,
                    2080.0,
                    2150.0,
                    2270.0,
                    2295.0,
                    2365.0,
                    2560.0,
                ],
                "rolling_avg_total_lipids": [
                    20.0,
                    21.0,
                    22.0,
                    23.0,
                    25.0,
                    27.0,
                    29.0,
                    31.0,
                ],
                "rolling_avg_total_carbs": [
                    100.0,
                    105.0,
                    110.0,
                    115.0,
                    125.0,
                    135.0,
                    145.0,
                    155.0,
                ],
                "rolling_avg_total_protein": [
                    50.0,
                    52.5,
                    55.0,
                    57.5,
                    62.5,
                    67.5,
                    72.5,
                    77.5,
                ],
            }
        )
        cls.test_data["date"] = pd.to_datetime(cls.test_data["date"])

        # Expected output after computing percentage changes
        cls.expected_output = cls.test_data.copy()
        cls.expected_output["weekday"] = cls.expected_output["date"].dt.day_name()

        # Adding manually calculated percentage changes
        cls.expected_output["percentage_change_total_calories"] = [
            0.0,
            4.76,
            -1.92,
            9.77,
            9.25,
            0.22,
            -1.90,
            22.66,
        ]
        cls.expected_output["percentage_change_total_lipids"] = [
            0.0,
            4.76,
            9.09,
            13.04,
            12.0,
            11.11,
            10.34,
            9.68,
        ]
        cls.expected_output["percentage_change_total_carbs"] = [
            0.0,
            4.76,
            9.09,
            13.04,
            12.0,
            11.11,
            10.34,
            9.68,
        ]
        cls.expected_output["percentage_change_total_protein"] = [
            0.0,
            4.76,
            9.09,
            13.04,
            12.0,
            11.11,
            10.34,
            9.68,
        ]

    def setUp(self):
        """
        Initializes instances for testing Pandas and DuckDB implementations.
        """
        self.pandas_change = PandasPercentageChange(dataframe=self.test_data)
        self.duckdb_change = DuckDBPercentageChange(dataframe=self.test_data)

    def test_pandas_percentage_change(self):
        """
        Test the percentage change calculation with the Pandas implementation.
        """
        result = self.pandas_change.compute_daily_percentage_change_pandas().round(2)
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
                    "percentage_change_total_calories",
                    "percentage_change_total_lipids",
                    "percentage_change_total_carbs",
                    "percentage_change_total_protein",
                ]
            ],
            self.expected_output,
            check_like=True,
        )
        print("Pandas percentage change test passed.")

    def test_duckdb_percentage_change(self):
        """
        Test the percentage change calculation with the DuckDB implementation.
        """
        result = self.duckdb_change.compute_daily_percentage_change_duckdb().round(2)
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
                    "percentage_change_total_calories",
                    "percentage_change_total_lipids",
                    "percentage_change_total_carbs",
                    "percentage_change_total_protein",
                ]
            ],
            self.expected_output,
            check_like=True,
        )
        print("DuckDB percentage change test passed.")


if __name__ == "__main__":
    unittest.main()
