# Adjusted group_data_test.py to use FILE_PATH
import os
import unittest
from data_engineering.Transform_data.group_data import pandas_group_data, duckdb_group_data, compare_results
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

# Base file path
FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data'

class TestPandasGroupData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Charger les données d'entrée
        cls.df = pd.read_excel(f'{FILE_PATH}\\combined_meal_data_filtered.xlsx')

        # Charger les résultats de sortie
        cls.daily_aggregation = pd.read_excel(f'{FILE_PATH}\\pandas_aggregation_results.xlsx', sheet_name='Daily Aggregation')
        cls.user_daily_aggregation = pd.read_excel(f'{FILE_PATH}\\pandas_aggregation_results.xlsx', sheet_name='User Daily Aggregation')
        cls.daily_mean_per_food = pd.read_excel(f'{FILE_PATH}\\pandas_aggregation_results.xlsx', sheet_name='Daily Mean Per Food')
        cls.user_food_grouping = pd.read_excel(f'{FILE_PATH}\\pandas_aggregation_results.xlsx', sheet_name='User Food Grouping')

    def test_data_loaded_correctly(self):
        # Vérifier que les données sont bien chargées
        self.assertFalse(self.df.empty, "Le fichier de données d'entrée ne doit pas être vide.")

    def test_date_conversion(self):
        # Vérifier que la colonne date est au bon format
        self.df['date'] = pd.to_datetime(self.df['date'])  # Assurez-vous que la colonne est bien en datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.df['date']), "La colonne 'date' doit être au format datetime.")

    def test_aggregation_by_day(self):
        # Vérifier que les agrégations par jour correspondent aux résultats de sortie
        expected_daily_aggregation = self.daily_aggregation

        # Refaire l'agrégation manuellement
        distinct_users_per_day = self.df.groupby('date')['user_id'].nunique().reset_index()
        distinct_users_per_day.columns = ['date', 'distinct_user_count']

        daily_aggregation = self.df.groupby('date').agg({
            'total_calories': 'sum',
            'total_lipids': 'sum',
            'total_carbs': 'sum',
            'total_protein': 'sum'
        }).reset_index()

        daily_aggregation = pd.merge(daily_aggregation, distinct_users_per_day, on='date')
        daily_aggregation['total_calories'] /= daily_aggregation['distinct_user_count']
        daily_aggregation['total_lipids'] /= daily_aggregation['distinct_user_count']
        daily_aggregation['total_carbs'] /= daily_aggregation['distinct_user_count']
        daily_aggregation['total_protein'] /= daily_aggregation['distinct_user_count']

        # Comparer avec les résultats attendus
        assert_frame_equal(daily_aggregation, expected_daily_aggregation)

    def test_aggregation_by_day_and_user(self):
        # Vérifier que l'agrégation par jour et utilisateur est correcte
        expected_user_daily_aggregation = self.user_daily_aggregation

        user_daily_aggregation = self.df.groupby(['date', 'user_id']).agg({
            'total_calories': 'sum',
            'total_lipids': 'sum',
            'total_carbs': 'sum',
            'total_protein': 'sum'
        }).reset_index()

        # Comparer avec les résultats attendus
        assert_frame_equal(user_daily_aggregation, expected_user_daily_aggregation)

    def test_average_nutrients_per_food(self):
        # Vérifier que la moyenne des nutriments par aliment est correcte
        expected_daily_mean_per_food = self.daily_mean_per_food

        daily_mean_per_food = self.df.groupby(['date', 'Aliment']).agg({
            'total_calories': 'mean',
            'total_lipids': 'mean',
            'total_carbs': 'mean',
            'total_protein': 'mean'
        }).reset_index()

        # Comparer avec les résultats attendus
        assert_frame_equal(daily_mean_per_food, expected_daily_mean_per_food)

    def test_grouping_by_user_and_food(self):
        # Vérifier que le groupement par utilisateur et aliment est correct
        expected_user_food_grouping = self.user_food_grouping

        user_food_grouping = self.df.groupby(['user_id', 'Aliment']).agg({
            'total_calories': 'sum',
            'total_lipids': 'sum',
            'total_carbs': 'sum',
            'total_protein': 'sum'
        }).reset_index()

        # Comparer avec les résultats attendus
        assert_frame_equal(user_food_grouping, expected_user_food_grouping)

if __name__ == '__main__':
    unittest.main()
