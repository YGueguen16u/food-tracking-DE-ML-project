"""
Unit tests for calculating rolling averages of meal data using both Pandas and DuckDB.
These tests validate that the rolling averages for the last 4 days are computed correctly
and the results match expectations for all specified metrics.
"""

import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from data_engineering.Transform_data.transform_4_2_windows_function_user import (
    DuckDBWindowFunction, PandasWindowFunction)


class TestWindowFunction(unittest.TestCase):
    """
    Unit tests for Pandas and DuckDB window functions calculating rolling averages.
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
                    1,  # user_id 1
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,  # user_id 2
                ],
                "total_calories": [
                    2000,
                    2100,
                    2200,
                    2300,
                    2400,
                    2500,
                    2600,
                    2700,  # user_id 1
                    1500,
                    1600,
                    1700,
                    1800,
                    1900,
                    2000,
                    2100,
                    2200,  # user_id 2
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
            }
        )
        cls.test_data["date"] = pd.to_datetime(cls.test_data["date"])

        # Expected output
        cls.expected_output = cls.test_data.copy()
        cls.expected_output["rolling_avg_total_calories"] = [
            2000.0,
            2050.0,
            2100.0,
            2150.0,  # user_id 1
            2250.0,
            2350.0,
            2450.0,
            2550.0,  # user_id 1
            1500.0,
            1550.0,
            1600.0,
            1650.0,  # user_id 2
            1750.0,
            1850.0,
            1950.0,
            2050.0,  # user_id 2
        ]
        cls.expected_output["rolling_avg_total_lipids"] = [
            20.0,
            21.0,
            22.0,
            23.0,  # user_id 1
            25.0,
            27.0,
            29.0,
            31.0,  # user_id 1
            15.0,
            16.0,
            17.0,
            18.0,  # user_id 2
            20.0,
            22.0,
            24.0,
            26.0,  # user_id 2
        ]
        cls.expected_output["rolling_avg_total_carbs"] = [
            100.0,
            102.5,
            105.0,
            107.5,  # user_id 1
            112.5,
            117.5,
            122.5,
            127.5,  # user_id 1
            90.0,
            92.5,
            95.0,
            97.5,  # user_id 2
            102.5,
            107.5,
            112.5,
            117.5,  # user_id 2
        ]
        cls.expected_output["rolling_avg_total_protein"] = [
            50.0,
            52.5,
            55.0,
            57.5,  # user_id 1
            62.5,
            67.5,
            72.5,
            77.5,  # user_id 1
            40.0,
            42.5,
            45.0,
            47.5,  # user_id 2
            52.5,
            57.5,
            62.5,
            67.5,  # user_id 2
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
        result = self.agg_pandas.compute_user_window_pandas().round(4)
        # Compare relevant columns
        assert_frame_equal(
            result[
                [
                    "user_id",
                    "date",
                    "rolling_avg_total_calories",
                    "rolling_avg_total_lipids",
                    "rolling_avg_total_carbs",
                    "rolling_avg_total_protein",
                ]
            ],
            self.expected_output[
                [
                    "user_id",
                    "date",
                    "rolling_avg_total_calories",
                    "rolling_avg_total_lipids",
                    "rolling_avg_total_carbs",
                    "rolling_avg_total_protein",
                ]
            ],
            check_like=True,
        )
        print("Pandas test passed.")

    def test_duckdb_window_function(self):
        """
        Test the rolling averages calculation with the DuckDB implementation.
        """
        result = self.agg_duckdb.compute_daily_window_duckdb().round(4)
        # Compare relevant columns
        assert_frame_equal(
            result[
                [
                    "user_id",
                    "date",
                    "rolling_avg_total_calories",
                    "rolling_avg_total_lipids",
                    "rolling_avg_total_carbs",
                    "rolling_avg_total_protein",
                ]
            ],
            self.expected_output[
                [
                    "user_id",
                    "date",
                    "rolling_avg_total_calories",
                    "rolling_avg_total_lipids",
                    "rolling_avg_total_carbs",
                    "rolling_avg_total_protein",
                ]
            ],
            check_like=True,
        )
        print("DuckDB test passed.")


if __name__ == "__main__":
    unittest.main()
