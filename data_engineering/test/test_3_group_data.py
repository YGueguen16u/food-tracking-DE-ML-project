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


class TestPandasAggregation(unittest.TestCase):
    """
    Classe de tests unitaires pour vérifier les agrégations réalisées avec Pandas.
    Utilise la classe PandasAggregation pour valider les méthodes d'agrégation sur
    des données alimentaires, incluant les agrégations par jour, par utilisateur et par aliment.
    """

    @classmethod
    def setUpClass(cls):
        """
        Initialise les données pour les tests d'agrégation avec Pandas en chargeant
        le DataFrame de base à partir du fichier 'combined_meal_data_filtered.xlsx'.
        """
        cls.df = pd.read_excel(f"{FILE_PATH}\\combined_meal_data_filtered.xlsx")
        cls.pandas_aggregation = PandasAggregation(cls.df)

    def test_aggregation_by_day(self):
        """
        Vérifie l'agrégation quotidienne des nutriments avec Pandas.
        Compare le résultat de la fonction aggregate_by_day() avec les résultats
        de référence stockés dans 'pandas_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="Daily Aggregation"
        )
        result = self.pandas_aggregation.aggregate_by_day()
        assert_frame_equal(result, expected)

    def test_aggregation_by_day_and_user(self):
        """
        Vérifie l'agrégation des nutriments par jour et utilisateur avec Pandas.
        Compare le résultat de la fonction aggregate_by_day_and_user() avec les résultats
        de référence stockés dans 'pandas_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
        )
        result = self.pandas_aggregation.aggregate_by_day_and_user()
        assert_frame_equal(result, expected)

    def test_average_nutrients_per_food(self):
        """
        Vérifie la moyenne des nutriments par aliment avec Pandas.
        Compare le résultat de la fonction average_nutrients_per_food() avec les résultats
        de référence stockés dans 'pandas_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="Daily Mean Per Food"
        )
        result = self.pandas_aggregation.average_nutrients_per_food()
        assert_frame_equal(result, expected)

    def test_grouping_by_user_and_food(self):
        """
        Vérifie le groupement des nutriments par utilisateur et aliment avec Pandas.
        Compare le résultat de la fonction group_by_user_and_food() avec les résultats
        de référence stockés dans 'pandas_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="User Food Grouping"
        )
        result = self.pandas_aggregation.group_by_user_and_food()
        assert_frame_equal(result, expected)


class TestDuckDBAggregation(unittest.TestCase):
    """
    Classe de tests unitaires pour vérifier les agrégations réalisées avec DuckDB.
    Utilise la classe DuckDBAggregation pour valider les méthodes d'agrégation sur
    des données alimentaires, incluant les agrégations par jour, par utilisateur et par aliment.
    """

    @classmethod
    def setUpClass(cls):
        """
        Initialise les données pour les tests d'agrégation avec DuckDB en chargeant
        le DataFrame de base à partir du fichier 'combined_meal_data_filtered.xlsx'.
        """
        cls.df = pd.read_excel(f"{FILE_PATH}\\combined_meal_data_filtered.xlsx")
        cls.duckdb_aggregation = DuckDBAggregation(cls.df)

    def test_aggregation_by_day(self):
        """
        Vérifie l'agrégation quotidienne des nutriments avec DuckDB.
        Compare le résultat de la fonction aggregate_by_day() avec les résultats
        de référence stockés dans 'duckdb_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="Daily Aggregation"
        )
        result = self.duckdb_aggregation.aggregate_by_day()
        assert_frame_equal(result, expected)

    def test_aggregation_by_day_and_user(self):
        """
        Vérifie l'agrégation des nutriments par jour et utilisateur avec DuckDB.
        Compare le résultat de la fonction aggregate_by_day_and_user() avec les résultats
        de référence stockés dans 'duckdb_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
        )
        result = self.duckdb_aggregation.aggregate_by_day_and_user()
        assert_frame_equal(result, expected)

    def test_average_nutrients_per_food(self):
        """
        Vérifie la moyenne des nutriments par aliment avec DuckDB.
        Compare le résultat de la fonction average_nutrients_per_food() avec les résultats
        de référence stockés dans 'duckdb_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="Daily Mean Per Food"
        )
        result = self.duckdb_aggregation.average_nutrients_per_food()
        assert_frame_equal(result, expected)

    def test_grouping_by_user_and_food(self):
        """
        Vérifie le groupement des nutriments par utilisateur et aliment avec DuckDB.
        Compare le résultat de la fonction group_by_user_and_food() avec les résultats
        de référence stockés dans 'duckdb_aggregation_results.xlsx'.
        """
        expected = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="User Food Grouping"
        )
        result = self.duckdb_aggregation.group_by_user_and_food()
        assert_frame_equal(result, expected)


class Comparison:
    """
    Classe utilitaire pour comparer les DataFrames entre les agrégations réalisées avec Pandas et DuckDB.
    """

    @staticmethod
    def compare_data(pandas_data, duckdb_data, sheet_name):
        """
        Compare deux DataFrames pour vérifier si les contenus sont identiques sur les colonnes communes.

        Args:
            pandas_data (DataFrame): DataFrame de l'agrégation réalisée avec Pandas.
            duckdb_data (DataFrame): DataFrame de l'agrégation réalisée avec DuckDB.
            sheet_name (str): Nom de la feuille en cours de comparaison.

        Returns:
            bool: True si les DataFrames sont identiques, False sinon. Enregistre les différences en CSV.
        """
        common_columns = pandas_data.columns.intersection(duckdb_data.columns).tolist()
        print(f"Comparaison des colonnes communes pour {sheet_name}: {common_columns}")

        merged_df = pandas_data[common_columns].merge(
            duckdb_data[common_columns], on=common_columns, suffixes=('_pandas', '_duckdb'), how="outer",
            indicator=True
        )

        differences = merged_df[merged_df["_merge"] != "both"]
        if not differences.empty:
            differences.to_csv(f"data/differences_{sheet_name}.csv", index=False)
        return differences.empty


class TestComparison(unittest.TestCase):
    """
    Tests unitaires pour la classe Comparison, vérifiant la correspondance
    entre les agrégations effectuées avec Pandas et DuckDB.
    """

    @classmethod
    def setUpClass(cls):
        """
        Charger les résultats attendus pour chaque type d'agrégation à partir des fichiers de référence.
        """
        cls.pandas_data_daily = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="Daily Aggregation"
        )
        cls.duckdb_data_daily = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="Daily Aggregation"
        )
        cls.pandas_data_user_daily = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
        )
        cls.duckdb_data_user_daily = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
        )
        cls.pandas_data_mean_food = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="Daily Mean Per Food"
        )
        cls.duckdb_data_mean_food = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="Daily Mean Per Food"
        )
        cls.pandas_data_user_food = pd.read_excel(
            f"{FILE_PATH}\\pandas_aggregation_results.xlsx", sheet_name="User Food Grouping"
        )
        cls.duckdb_data_user_food = pd.read_excel(
            f"{FILE_PATH}\\duckdb_aggregation_results.xlsx", sheet_name="User Food Grouping"
        )

    def test_compare_daily_aggregation(self):
        """
        Test de comparaison des agrégations quotidiennes entre Pandas et DuckDB.
        """
        self.assertTrue(
            Comparison.compare_data(
                self.pandas_data_daily,
                self.duckdb_data_daily,
                "Daily Aggregation"
            ),
            "Les agrégations quotidiennes ne correspondent pas entre Pandas et DuckDB."
        )

    def test_compare_user_daily_aggregation(self):
        """
        Test de comparaison des agrégations par utilisateur et par jour entre Pandas et DuckDB.
        """
        self.assertTrue(
            Comparison.compare_data(
                self.pandas_data_user_daily,
                self.duckdb_data_user_daily,
                "User Daily Aggregation"
            ),
            "Les agrégations par utilisateur et par jour ne correspondent pas entre Pandas et DuckDB."
        )

    def test_compare_average_nutrients_per_food(self):
        """
        Test de comparaison des moyennes des nutriments par aliment entre Pandas et DuckDB.
        """
        self.assertTrue(
            Comparison.compare_data(
                self.pandas_data_mean_food,
                self.duckdb_data_mean_food,
                "Daily Mean Per Food"
            ),
            "Les moyennes des nutriments par aliment ne correspondent pas entre Pandas et DuckDB."
        )

    def test_compare_grouping_by_user_and_food(self):
        """
        Test de comparaison des groupements par utilisateur et par aliment entre Pandas et DuckDB.
        """
        self.assertTrue(
            Comparison.compare_data(
                self.pandas_data_user_food,
                self.duckdb_data_user_food,
                "User Food Grouping"
            ),
            "Les groupements par utilisateur et aliment ne correspondent pas entre Pandas et DuckDB."
        )


if __name__ == "__main__":
    unittest.main()
