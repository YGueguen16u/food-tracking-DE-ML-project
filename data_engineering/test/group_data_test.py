"""
Ce module effectue des tests unitaires pour comparer
l'agrégation des données alimentaires avec Pandas et DuckDB.
Les tests comparent les résultats obtenus pour les agrégations quotidiennes,
les agrégations par utilisateur et par aliment, et vérifient que les
résultats correspondent aux fichiers de référence.
"""

import unittest

import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

# Base file path
FILE_PATH = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data"


class TestPandasGroupData(unittest.TestCase):
    """
    Classe de test pour vérifier les fonctions d'agrégation des données alimentaires
    à l'aide de Pandas. Ces tests comparent les résultats agrégés par jour,
    par jour et utilisateur, et par aliment.
    """

    @classmethod
    def setUpClass(cls):
        """
        Méthode appelée une fois avant tous les tests pour charger les données
        d'entrée et les résultats attendus pour l'agrégation avec Pandas.
        """
        cls.df = pd.read_excel(f"{FILE_PATH}\\combined_meal_data_filtered.xlsx")

        # Charger les résultats de sortie pour comparaison
        cls.daily_aggregation = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx",
            sheet_name="Daily Aggregation",
        )
        cls.user_daily_aggregation = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx",
            sheet_name="User Daily Aggregation",
        )
        cls.daily_mean_per_food = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx",
            sheet_name="Daily Mean Per Food",
        )
        cls.user_food_grouping = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx",
            sheet_name="User Food Grouping",
        )

    def test_data_loaded_correctly(self):
        """
        Vérifie que les données d'entrée sont correctement chargées
        et ne sont pas vides.
        """
        self.assertFalse(
            self.df.empty, "Le fichier de données d'entrée ne doit pas être vide."
        )

    def test_date_conversion(self):
        """
        Vérifie que la colonne 'date' est convertie correctement en format datetime.
        """
        self.df["date"] = pd.to_datetime(self.df["date"])
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(self.df["date"]),
            "La colonne 'date' doit être au format datetime.",
        )

    def test_aggregation_by_day(self):
        """
        Vérifie que les résultats de l'agrégation par jour correspondent
        aux résultats de sortie attendus.
        """
        expected_daily_aggregation = self.daily_aggregation

        # Calculer l'agrégation manuellement
        distinct_users_per_day = (
            self.df.groupby("date")["user_id"].nunique().reset_index()
        )
        distinct_users_per_day.columns = ["date", "distinct_user_count"]

        daily_aggregation = (
            self.df.groupby("date")
            .agg(
                {
                    "total_calories": "sum",
                    "total_lipids": "sum",
                    "total_carbs": "sum",
                    "total_protein": "sum",
                }
            )
            .reset_index()
        )

        daily_aggregation = pd.merge(
            daily_aggregation, distinct_users_per_day, on="date"
        )
        daily_aggregation["total_calories"] /= daily_aggregation["distinct_user_count"]
        daily_aggregation["total_lipids"] /= daily_aggregation["distinct_user_count"]
        daily_aggregation["total_carbs"] /= daily_aggregation["distinct_user_count"]
        daily_aggregation["total_protein"] /= daily_aggregation["distinct_user_count"]

        # Comparer avec les résultats attendus
        assert_frame_equal(daily_aggregation, expected_daily_aggregation)

    def test_aggregation_by_day_and_user(self):
        """
        Vérifie que les résultats de l'agrégation par jour et par utilisateur
        correspondent aux résultats de sortie attendus.
        """
        expected_user_daily_aggregation = self.user_daily_aggregation

        user_daily_aggregation = (
            self.df.groupby(["date", "user_id"])
            .agg(
                {
                    "total_calories": "sum",
                    "total_lipids": "sum",
                    "total_carbs": "sum",
                    "total_protein": "sum",
                }
            )
            .reset_index()
        )

        # Comparer avec les résultats attendus
        assert_frame_equal(user_daily_aggregation, expected_user_daily_aggregation)

    def test_average_nutrients_per_food(self):
        """
        Vérifie que les moyennes des nutriments par aliment correspondent
        aux résultats de sortie attendus.
        """
        expected_daily_mean_per_food = self.daily_mean_per_food

        daily_mean_per_food = (
            self.df.groupby(["date", "Aliment"])
            .agg(
                {
                    "total_calories": "mean",
                    "total_lipids": "mean",
                    "total_carbs": "mean",
                    "total_protein": "mean",
                }
            )
            .reset_index()
        )

        # Comparer avec les résultats attendus
        assert_frame_equal(daily_mean_per_food, expected_daily_mean_per_food)

    def test_grouping_by_user_and_food(self):
        """
        Vérifie que les résultats du groupement par utilisateur et par aliment
        correspondent aux résultats de sortie attendus.
        """
        expected_user_food_grouping = self.user_food_grouping

        user_food_grouping = (
            self.df.groupby(["user_id", "Aliment"])
            .agg(
                {
                    "total_calories": "sum",
                    "total_lipids": "sum",
                    "total_carbs": "sum",
                    "total_protein": "sum",
                }
            )
            .reset_index()
        )

        # Comparer avec les résultats attendus
        assert_frame_equal(user_food_grouping, expected_user_food_grouping)


class TestDuckDBGroupData(unittest.TestCase):
    """
    Classe de test pour vérifier les fonctions d'agrégation des données alimentaires
    à l'aide de DuckDB. Ces tests comparent les résultats agrégés par jour,
    par jour et utilisateur, et par aliment.
    """

    @classmethod
    def setUpClass(cls):
        """
        Méthode appelée une fois avant tous les tests pour charger les données
        d'entrée et les résultats attendus pour l'agrégation avec DuckDB.
        """
        cls.df = pd.read_excel(f"{FILE_PATH}\\combined_meal_data_filtered.xlsx")

        # Charger les résultats de sortie pour comparaison
        cls.daily_aggregation = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx",
            sheet_name="Daily Aggregation",
        )
        cls.user_daily_aggregation = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx",
            sheet_name="User Daily Aggregation",
        )
        cls.daily_mean_per_food = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx",
            sheet_name="Daily Mean Per Food",
        )
        cls.user_food_grouping = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx",
            sheet_name="User Food Grouping",
        )

    def test_data_loaded_correctly(self):
        """
        Vérifie que les données d'entrée sont correctement chargées
        et ne sont pas vides.
        """
        self.assertFalse(
            self.df.empty, "Le fichier de données d'entrée ne doit pas être vide."
        )

    def test_date_conversion(self):
        """
        Vérifie que la colonne 'date' est convertie correctement en format datetime.
        """
        self.df["date"] = pd.to_datetime(self.df["date"])
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(self.df["date"]),
            "La colonne 'date' doit être au format datetime.",
        )

    def test_aggregation_by_day(self):
        """
        Vérifie que les résultats de l'agrégation par jour
        avec DuckDB correspondent aux résultats de sortie attendus.
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("df", self.df)

        expected_daily_aggregation = self.daily_aggregation

        daily_aggregation = conn.execute(
            """
            SELECT 
                date,
                SUM(total_calories) / COUNT(DISTINCT user_id) AS avg_calories_per_user,
                SUM(total_lipids) / COUNT(DISTINCT user_id) AS avg_lipids_per_user,
                SUM(total_carbs) / COUNT(DISTINCT user_id) AS avg_carbs_per_user,
                SUM(total_protein) / COUNT(DISTINCT user_id) AS avg_protein_per_user
            FROM df
            GROUP BY date
            ORDER BY date
            """
        ).df()

        # Comparer avec les résultats attendus
        assert_frame_equal(daily_aggregation, expected_daily_aggregation)

    def test_aggregation_by_day_and_user(self):
        """
        Vérifie que les résultats de l'agrégation par jour et par utilisateur avec DuckDB
        correspondent aux résultats de sortie attendus.
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("df", self.df)

        expected_user_daily_aggregation = self.user_daily_aggregation

        user_daily_aggregation = conn.execute(
            """
            SELECT date,
                   user_id,
                   SUM(total_calories) AS total_calories,
                   SUM(total_lipids) AS total_lipids,
                   SUM(total_carbs) AS total_carbs,
                   SUM(total_protein) AS total_protein
            FROM df
            GROUP BY date, user_id
            """
        ).df()

        # Ensure 'date' is in datetime format for both DataFrames and remove time component
        user_daily_aggregation["date"] = pd.to_datetime(
            user_daily_aggregation["date"]
        ).dt.date
        expected_user_daily_aggregation["date"] = pd.to_datetime(
            expected_user_daily_aggregation["date"]
        ).dt.date

        # Ensure 'user_id' is of the same type in both DataFrames
        user_daily_aggregation["user_id"] = user_daily_aggregation["user_id"].astype(
            int
        )
        expected_user_daily_aggregation["user_id"] = expected_user_daily_aggregation[
            "user_id"
        ].astype(int)

        # Sort both DataFrames to ensure they have the same order before comparing
        user_daily_aggregation = user_daily_aggregation.sort_values(
            by=["date", "user_id"]
        ).reset_index(drop=True)
        expected_user_daily_aggregation = expected_user_daily_aggregation.sort_values(
            by=["date", "user_id"]
        ).reset_index(drop=True)

        # Compare with expected results
        assert_frame_equal(user_daily_aggregation, expected_user_daily_aggregation)

    def test_average_nutrients_per_food(self):
        """
        Vérifie que les moyennes des nutriments par aliment avec DuckDB
        correspondent aux résultats de sortie attendus.
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("df", self.df)

        expected_daily_mean_per_food = self.daily_mean_per_food

        daily_mean_per_food = conn.execute(
            """
            SELECT date,
                   Aliment,
                   AVG(total_calories) AS avg_calories,
                   AVG(total_lipids) AS avg_lipids,
                   AVG(total_carbs) AS avg_carbs,
                   AVG(total_protein) AS avg_protein
            FROM df
            WHERE Aliment IS NOT NULL
            GROUP BY date, Aliment
            """
        ).df()

        # Ensure 'date' is in datetime format and normalized to remove time
        daily_mean_per_food["date"] = pd.to_datetime(
            daily_mean_per_food["date"]
        ).dt.normalize()
        expected_daily_mean_per_food["date"] = pd.to_datetime(
            expected_daily_mean_per_food["date"]
        ).dt.normalize()

        # Convert dates to string format (optional, to ensure consistent representation)
        daily_mean_per_food["date"] = daily_mean_per_food["date"].dt.strftime(
            "%Y-%m-%d"
        )
        expected_daily_mean_per_food["date"] = expected_daily_mean_per_food[
            "date"
        ].dt.strftime("%Y-%m-%d")

        # Sort both DataFrames by 'date' and 'Aliment' before comparing
        daily_mean_per_food = daily_mean_per_food.sort_values(
            by=["date", "Aliment"]
        ).reset_index(drop=True)
        expected_daily_mean_per_food = expected_daily_mean_per_food.sort_values(
            by=["date", "Aliment"]
        ).reset_index(drop=True)

        # Compare with expected results
        assert_frame_equal(daily_mean_per_food, expected_daily_mean_per_food)

    def test_grouping_by_user_and_food(self):
        """
        Vérifie que les résultats du groupement par utilisateur et par aliment avec DuckDB
        correspondent aux résultats de sortie attendus.
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("df", self.df)

        expected_user_food_grouping = self.user_food_grouping

        user_food_grouping = conn.execute(
            """
            SELECT user_id,
                   Aliment,
                   SUM(total_calories) AS total_calories,
                   SUM(total_lipids) AS total_lipids,
                   SUM(total_carbs) AS total_carbs,
                   SUM(total_protein) AS total_protein
            FROM df
            WHERE Aliment IS NOT NULL
            GROUP BY user_id, Aliment
            """
        ).df()

        # Ensure 'user_id' is of the same type and sorted
        user_food_grouping["user_id"] = user_food_grouping["user_id"].astype(int)
        expected_user_food_grouping["user_id"] = expected_user_food_grouping[
            "user_id"
        ].astype(int)

        # Sort both DataFrames by 'user_id' and 'Aliment' before comparing
        user_food_grouping = user_food_grouping.sort_values(
            by=["user_id", "Aliment"]
        ).reset_index(drop=True)
        expected_user_food_grouping = expected_user_food_grouping.sort_values(
            by=["user_id", "Aliment"]
        ).reset_index(drop=True)

        # Compare with expected results
        assert_frame_equal(user_food_grouping, expected_user_food_grouping)


if __name__ == "__main__":
    unittest.main()
