"""
Ce module effectue des tests unitaires pour comparer
l'agrégation des données alimentaires avec Pandas et DuckDB.
Les tests comparent les résultats obtenus pour les agrégations quotidiennes,
les agrégations par utilisateur et par aliment, et vérifient que les
résultats correspondent aux fichiers de référence.
"""

import unittest
import pandas as pd
import duckdb
from pandas.testing import assert_frame_equal
from data_engineering.Transform_data.group_data import PandasAggregation, DuckDBAggregation

# Base file path
FILE_PATH = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data"


class TestDataSetup(unittest.TestCase):
    """
    Sets up test data for testing aggregation functions.
    This data is shared across Pandas and DuckDB test classes.
    """
    @classmethod
    def setUpClass(cls):
        # Create test data frame for testing
        cls.test_dataframe = pd.DataFrame({
            "meal_record_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "user_id": [1, 1, 2, 2,
                        3, 3, 4, 4,
                        1, 2, 3, 4],
            "meal_id": [1, 2, 1, 2,
                        1, 2, 1, 2,
                        1, 1, 1, 1],
            "date": ["2024-04-01", "2024-04-01", "2024-04-01", "2024-04-01",
                     "2024-04-02", "2024-04-02", "2024-04-02", "2024-04-02",
                     "2024-04-02", "2024-04-02", "2024-04-03", "2024-04-03"],
            "heure": ["07:02:00", "12:00:00", "08:00:00", "13:00:00",
                      "07:30:00", "12:30:00", "08:15:00", "13:15:00",
                      "18:30:00", "19:00:00", "18:45:00", "19:15:00"],
            "aliment_id": [453, 454, 455, 456, 457, 458, 459, 460, 453, 455, 457, 459],
            "quantity": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "Aliment": ["Cacahuètes, crues", "Cacahuètes, crues", "Amandes, crues", "Amandes, crues",
                        "Concombre", "Concombre", "Steak", "Steak",
                        "Cacahuètes, crues", "Amandes, crues", "Concombre", "Steak"],
            "Valeur calorique": [828, 828, 600, 600,
                                 20, 20, 250, 250,
                                 828, 600, 20, 250],
            "Lipides": [71.9, 71.9, 50.5, 50.5, 0.1, 0.1, 15.0, 15.0, 71.9, 50.5, 0.1, 15.0],
            "Glucides": [23.5, 23.5, 20.0, 20.0, 4.0, 4.0, 0.0, 0.0, 23.5, 20.0, 4.0, 0.0],
            "Protein": [37.7, 37.7, 25.0, 25.0, 0.6, 0.6, 22.0, 22.0, 37.7, 25.0, 0.6, 22.0],
            "total_calories": [828, 828, 600, 600, 20, 20, 250, 250, 828, 600, 20, 250],
            "total_lipids": [71.9, 71.9, 50.5, 50.5, 0.1, 0.1, 15.0, 15.0, 71.9, 50.5, 0.1, 15.0],
            "total_carbs": [23.5, 23.5, 20.0, 20.0, 4.0, 4.0, 0.0, 0.0, 23.5, 20.0, 4.0, 0.0],
            "total_protein": [37.7, 37.7, 25.0, 25.0, 0.6, 0.6, 22.0, 22.0, 37.7, 25.0, 0.6, 22.0],
            "extreme_quantity": ["Normal", "Normal", "Normal", "Normal", "Normal", "Normal", "Normal", "Normal",
                                 "Normal", "Normal", "Normal", "Normal"]
        })

        cls.test_food_type_data = pd.DataFrame({
            "Aliment": ["Cacahuètes, crues", "Amandes, crues", "Concombre", "Steak"],
            "Type": ["Noix", "Noix", "Légume", "Viande"]
        })

        # Expected results for each aggregation
        cls.expected_daily_agg = pd.DataFrame({
            "date": ["2024-04-01", "2024-04-02", "2024-04-03"],
            "total_calories": [1428.0, 492.0, 135.0],
            "total_lipids": [122.4, 38.15, 7.55],
            "total_carbs": [43.5, 12.875, 2.0],
            "total_protein": [62.7, 26.975, 11.3],
            "distinct_user_count": [2, 4, 2]
        })

        cls.expected_user_daily_agg = pd.DataFrame({
            "date": ["2024-04-01", "2024-04-01", "2024-04-02", "2024-04-02", "2024-04-02", "2024-04-02", "2024-04-03",
                     "2024-04-03"],
            "user_id": [1, 2, 1, 2, 3, 4, 3, 4],
            "total_calories": [1656, 1200, 828, 600, 40, 500, 20, 250],
            "total_lipids": [143.8, 101.0, 71.9, 50.5, 0.2, 30.0, 0.1, 15.0],
            "total_carbs": [47.0, 40.0, 23.5, 20.0, 8.0, 0.0, 4.0, 0.0],
            "total_protein": [75.4, 50.0, 37.7, 25.0, 1.2, 44.0, 0.6, 22.0]
        })

        cls.expected_daily_mean_per_food = pd.DataFrame({
            "date": ["2024-04-01", "2024-04-01", "2024-04-02",
                     "2024-04-02", "2024-04-02", "2024-04-02",
                     "2024-04-03", "2024-04-03"],
            "Aliment": ["Amandes, crues", "Cacahuètes, crues", "Amandes, crues", "Cacahuètes, crues", "Concombre", "Steak", "Concombre", "Steak"],
            "total_calories": [600.0, 828.0, 600.0, 828.0, 20.0, 250.0, 20.0, 250.0],
            "total_lipids": [50.5, 71.9, 50.5, 71.9, 0.1, 15.0, 0.1, 15.0],
            "total_carbs": [20.0, 23.5, 20.0, 23.5, 4.0, 0.0, 4.0, 0.0],
            "total_protein": [25.0, 37.7, 25.0, 37.7, 0.6, 22.0, 0.6, 22.0]

        })

        cls.expected_user_food_type_grouping = pd.DataFrame({
            "user_id": [1, 2, 3, 4],
            "Type": ["Noix", "Noix", "Légume", "Viande"],
            "total_calories": [2484, 1800, 60, 750],
            "total_lipids": [215.7, 151.5, 0.3, 45.0],
            "total_carbs": [70.5, 60.0, 12.0, 0.0],
            "total_protein": [113.1, 75.0, 1.8, 66.0],
            "food_count": [3, 3, 3, 3],
            "avg_calories_per_type": [828.0, 600.0, 20.0, 250.0],
            "avg_lipids_per_type": [71.9, 50.5, 0.1, 15.0],
            "avg_carbs_per_type": [23.5, 20.0, 4.0, 0.0],
            "avg_protein_per_type": [37.7, 25.0, 0.6, 22.0]
        })


class TestPandasAggregation(TestDataSetup):
    def setUp(self):
        # Initialize PandasAggregation with test data
        self.agg = PandasAggregation(dataframe=self.test_dataframe, food_type_data=self.test_food_type_data)

    def test_daily_aggregation(self):
        result = self.agg.daily_aggregation().round(4)
        print("Résultat aggregation:\n", result)
        print("Valeurs attendues:\n", self.expected_daily_agg)
        assert_frame_equal(result, self.expected_daily_agg)

    def test_user_daily_aggregation(self):
        result = self.agg.user_daily_aggregation().round(4)
        print("Résultat aggregation:\n", result)
        print("Valeurs attendues:\n", self.expected_user_daily_agg)
        assert_frame_equal(result, self.expected_user_daily_agg)

    def test_daily_mean_per_food(self):
        result = self.agg.daily_mean_per_food().round(4)
        print("Résultat aggregation:\n", result)
        print("Valeurs attendues:\n", self.expected_daily_mean_per_food)
        assert_frame_equal(result, self.expected_daily_mean_per_food)

    def test_user_food_type_grouping(self):
        result = self.agg.user_food_type_grouping().round(4)
        print("Résultat aggregation:\n", result)
        print("Valeurs attendues:\n", self.expected_user_food_type_grouping)
        assert_frame_equal(result, self.expected_user_food_type_grouping)


class TestDuckDBAggregation(TestDataSetup):
    def setUp(self):
        # Initialize DuckDBAggregation with test data
        self.agg = DuckDBAggregation(dataframe=self.test_dataframe, food_type_data=self.test_food_type_data)

    def test_daily_aggregation(self):
        result_daily = self.agg.daily_aggregation().round(4)
        print("Résultat aggregation:\n", result_daily)
        print("Valeurs attendues:\n", self.expected_daily_agg)
        assert_frame_equal(result_daily, self.expected_daily_agg)

    def test_user_daily_aggregation(self):
        # Cast result and expected data to float for relevant columns
        result_user_daily = self.agg.user_daily_aggregation().astype({
            'total_calories': 'float',
            'total_lipids': 'float',
            'total_carbs': 'float',
            'total_protein': 'float'
        }).round(4)
        expected = self.expected_user_daily_agg.astype({
            'total_calories': 'float',
            'total_lipids': 'float',
            'total_carbs': 'float',
            'total_protein': 'float'
        }).round(4)
        print("Résultat aggregation:\n", result_user_daily)
        print("Valeurs attendues:\n", expected)

        # Assert equality of frames with float type
        assert_frame_equal(result_user_daily, expected)

    def test_daily_mean_per_food(self):
        result_mean_food = self.agg.daily_mean_per_food().round(4)
        print("Résultat aggregation:\n", result_mean_food)
        print("Valeurs attendues:\n", self.expected_daily_mean_per_food)
        assert_frame_equal(result_mean_food, self.expected_daily_mean_per_food)

    def test_user_food_type_grouping(self):
        result_user_food = self.agg.user_food_type_grouping().round(4)

        # Convert relevant columns of expected DataFrame to float
        expected = self.expected_user_food_type_grouping.astype({
            "total_calories": "float",
            "total_lipids": "float",
            "total_carbs": "float",
            "total_protein": "float",
            "avg_calories_per_type": "float",
            "avg_lipids_per_type": "float",
            "avg_carbs_per_type": "float",
            "avg_protein_per_type": "float"
        }).round(4)

        print("Résultat aggregation:\n", result_user_food)
        print("Valeurs attendues:\n", expected)

        # Assert equality of frames with consistent types
        assert_frame_equal(result_user_food, expected)


if __name__ == "__main__":
    unittest.main()
