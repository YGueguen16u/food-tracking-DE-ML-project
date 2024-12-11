"""
Unit tests for computing nutrient proportions per user and food type.
Tests validate correct calculation of proportions using Pandas and DuckDB.
Ensures consistency between implementations for all nutrient metrics.
"""

import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from data_engineering.Transform_data.window_function_type_food import (
    DuckDBProportionCalculation, PandasProportionCalculation)


class TestProportionCalculation(unittest.TestCase):
    """
    Unit tests for Pandas and DuckDB proportion calculations per user and food type.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up test data for both Pandas and DuckDB implementations.
        """
        cls.test_data = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 1, 2, 2, 2, 2],
                "Type": ["A", "B", "C", "D", "A", "B", "C", "E"],
                "total_calories": [400, 600, 300, 700, 800, 900, 600, 850],
                "total_lipids": [10, 20, 15, 15, 20, 35, 25, 15],
                "total_protein": [30, 40, 35, 35, 45, 65, 45, 35],
                "total_carbs": [50, 50, 60, 40, 55, 60, 40, 60],
            }
        )
        cls.expected_output = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 1, 2, 2, 2, 2],
                "Type": ["A", "B", "C", "D", "A", "B", "C", "E"],
                "proportion_total_calories": [
                    0.2,
                    0.3,
                    0.15,
                    0.35,
                    0.254,
                    0.2857,
                    0.1905,
                    0.2698,
                ],
                "proportion_total_lipids": [
                    0.1667,
                    0.3333,
                    0.25,
                    0.25,
                    0.2105,
                    0.3684,
                    0.2632,
                    0.1579,
                ],
                "proportion_total_protein": [
                    0.2143,
                    0.2857,
                    0.25,
                    0.25,
                    0.2368,
                    0.3421,
                    0.2368,
                    0.1842,
                ],
                "proportion_total_carbs": [
                    0.25,
                    0.25,
                    0.3,
                    0.2,
                    0.2558,
                    0.2791,
                    0.186,
                    0.2791,
                ],
            }
        )

    def setUp(self):
        """
        Initialize instances for Pandas and DuckDB implementations.
        """
        self.pandas_calc = PandasProportionCalculation(dataframe=self.test_data)
        self.duckdb_calc = DuckDBProportionCalculation(dataframe=self.test_data)

    def test_pandas_proportion_calculation(self):
        """
        Test proportion calculations using the Pandas implementation.
        """
        result = self.pandas_calc.compute_proportions_pandas().round(4)
        assert_frame_equal(
            result[
                [
                    "user_id",
                    "Type",
                    "proportion_total_calories",
                    "proportion_total_lipids",
                    "proportion_total_protein",
                    "proportion_total_carbs",
                ]
            ],
            self.expected_output[
                [
                    "user_id",
                    "Type",
                    "proportion_total_calories",
                    "proportion_total_lipids",
                    "proportion_total_protein",
                    "proportion_total_carbs",
                ]
            ],
            check_like=True,
        )
        print("Pandas proportion test passed.")

    def test_duckdb_proportion_calculation(self):
        """
        Test proportion calculations using the DuckDB implementation.
        """
        result = self.duckdb_calc.compute_proportions_duckdb().round(4)
        assert_frame_equal(
            result[
                [
                    "user_id",
                    "Type",
                    "proportion_total_calories",
                    "proportion_total_lipids",
                    "proportion_total_protein",
                    "proportion_total_carbs",
                ]
            ],
            self.expected_output[
                [
                    "user_id",
                    "Type",
                    "proportion_total_calories",
                    "proportion_total_lipids",
                    "proportion_total_protein",
                    "proportion_total_carbs",
                ]
            ],
            check_like=True,
        )
        print("DuckDB proportion test passed.")


if __name__ == "__main__":
    unittest.main()
