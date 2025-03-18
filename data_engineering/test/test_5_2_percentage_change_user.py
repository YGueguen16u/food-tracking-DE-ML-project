"""
Unit tests for nutrient percentage changes grouped by user_id using Pandas and DuckDB.
These tests validate that percentage changes relative to rolling averages are computed correctly
and the results match expectations for all specified metrics.
"""

import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from data_engineering.Transform_data.transform_5_2_percentage_change_user import (
    DuckDBPercentageChange, PandasPercentageChange)


class TestUserPercentageChange(unittest.TestCase):
    """
    Unit tests for Pandas and DuckDB percentage change calculations grouped by user_id.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up test data for both Pandas and DuckDB implementations.
        """
        cls.test_data = pd.DataFrame(
            {
                "date": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-06",
                    "2024-01-07",
                    "2024-01-08",
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-10",
                    "2024-01-17",
                    "2024-01-24",
                    "2024-01-31",
                ],
                "user_id": [
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                ],
                "total_calories": [
                    2000,
                    2100,
                    2200,
                    2300,
                    2400,
                    2500,
                    2600,
                    2700,
                    1500,
                    1600,
                    1700,
                    1800,
                    1900,
                    2000,
                    2100,
                    2200,
                ],
                "total_lipids": [
                    20,
                    22,
                    24,
                    26,
                    28,
                    30,
                    32,
                    34,
                    15,
                    17,
                    19,
                    21,
                    23,
                    25,
                    27,
                    29,
                ],
                "total_carbs": [
                    100,
                    105,
                    110,
                    115,
                    120,
                    125,
                    130,
                    135,
                    90,
                    95,
                    100,
                    105,
                    110,
                    115,
                    120,
                    125,
                ],
                "total_protein": [
                    50,
                    55,
                    60,
                    65,
                    70,
                    75,
                    80,
                    85,
                    40,
                    45,
                    50,
                    55,
                    60,
                    65,
                    70,
                    75,
                ],
                "rolling_avg_total_calories": [
                    2000.0,
                    2050.0,
                    2100.0,
                    2150.0,
                    2250.0,
                    2350.0,
                    2450.0,
                    2550.0,
                    1500.0,
                    1550.0,
                    1600.0,
                    1650.0,
                    1750.0,
                    1850.0,
                    1950.0,
                    2050.0,
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
                    15.0,
                    16.0,
                    17.0,
                    18.0,
                    20.0,
                    22.0,
                    24.0,
                    26.0,
                ],
                "rolling_avg_total_carbs": [
                    100.0,
                    102.5,
                    105.0,
                    107.5,
                    112.5,
                    117.5,
                    122.5,
                    127.5,
                    90.0,
                    92.5,
                    95.0,
                    97.5,
                    102.5,
                    107.5,
                    112.5,
                    117.5,
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
                    40.0,
                    42.5,
                    45.0,
                    47.5,
                    52.5,
                    57.5,
                    62.5,
                    67.5,
                ],
            }
        )

        cls.test_data["date"] = pd.to_datetime(
            cls.test_data["date"]
        )  # Ensure datetime format
        cls.expected_output = cls.test_data.copy()
        cls.expected_output["percentage_change_total_calories"] = [
            0.0,
            2.44,
            4.76,
            6.98,
            6.67,
            6.38,
            6.12,
            5.88,
            0.0,
            3.23,
            6.25,
            9.09,
            8.57,
            8.11,
            7.69,
            7.32,
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
            0.0,
            6.25,
            11.76,
            16.67,
            15.0,
            13.64,
            12.5,
            11.54,
        ]
        cls.expected_output["percentage_change_total_carbs"] = [
            0.0,
            2.44,
            4.76,
            6.98,
            6.67,
            6.38,
            6.12,
            5.88,
            0.0,
            2.7,
            5.26,
            7.69,
            7.32,
            6.98,
            6.67,
            6.38,
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
            0.0,
            5.88,
            11.11,
            15.79,
            14.29,
            13.04,
            12.0,
            11.11,
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
        result = self.pandas_change.compute_user_percentage_change_pandas().round(2)
        assert_frame_equal(
            result[
                [
                    "user_id",
                    "date",
                    "total_calories",
                    "rolling_avg_total_calories",
                    "percentage_change_total_calories",
                    "rolling_avg_total_lipids",
                    "percentage_change_total_lipids",
                    "rolling_avg_total_carbs",
                    "percentage_change_total_carbs",
                    "rolling_avg_total_protein",
                    "percentage_change_total_protein",
                ]
            ],
            self.expected_output[
                [
                    "user_id",
                    "date",
                    "total_calories",
                    "rolling_avg_total_calories",
                    "percentage_change_total_calories",
                    "rolling_avg_total_lipids",
                    "percentage_change_total_lipids",
                    "rolling_avg_total_carbs",
                    "percentage_change_total_carbs",
                    "rolling_avg_total_protein",
                    "percentage_change_total_protein",
                ]
            ],
            check_like=True,
        )
        print("Pandas percentage change test passed.")

    def test_duckdb_percentage_change(self):
        """
        Test the percentage change calculation with the DuckDB implementation.
        """
        result = self.duckdb_change.compute_user_percentage_change_duckdb().round(2)
        assert_frame_equal(
            result[
                [
                    "user_id",
                    "date",
                    "total_calories",
                    "rolling_avg_total_calories",
                    "percentage_change_total_calories",
                    "rolling_avg_total_lipids",
                    "percentage_change_total_lipids",
                    "rolling_avg_total_carbs",
                    "percentage_change_total_carbs",
                    "rolling_avg_total_protein",
                    "percentage_change_total_protein",
                ]
            ],
            self.expected_output[
                [
                    "user_id",
                    "date",
                    "total_calories",
                    "rolling_avg_total_calories",
                    "percentage_change_total_calories",
                    "rolling_avg_total_lipids",
                    "percentage_change_total_lipids",
                    "rolling_avg_total_carbs",
                    "percentage_change_total_carbs",
                    "rolling_avg_total_protein",
                    "percentage_change_total_protein",
                ]
            ],
            check_like=True,
        )
        print("DuckDB percentage change test passed.")

    if __name__ == "__main__":
        unittest.main()
