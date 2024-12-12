"""
This script calculates percentage changes in nutrients for meal data grouped by food type.
The changes use the rolling average of the last 4 occurrences of each food type per user.
It generates Excel files with the processed data for further analysis.
"""

import os

import duckdb
import pandas as pd


class PandasPercentageChange:
    """
    Class to compute percentage changes in nutrient values compared to the average of the last
    4 occurrences of each food type per user using Pandas.

    Methods:
        compute_food_type_percentage_change_pandas: Computes percentage changes for a DataFrame.
        pandas_percentage_change_food_type: Saves the processed data into an Excel file.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset."""
        if dataframe is not None:
            self.dataframe = dataframe
        else: 
            self.dataframe = pd.read_excel(
                "data/user_food_proportion_pandas.xlsx",
                sheet_name="Proportion Results",
            )

    def compute_food_type_percentage_change_pandas(self):
        """
        Computes percentage changes of nutrient values compared to the rolling average of the last
        4 occurrences of each food type per user using Pandas.

        Returns:
            DataFrame: DataFrame with percentage changes and rolling averages added.
        """
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])
        food_type_dataframe = self.dataframe.sort_values(["user_id", "Type", "date"])

        # Liste des colonnes de nutriments à traiter
        nutrient_cols = ["total_calories", "total_lipids", "total_carbs", "total_protein",
                        "proportion_total_calories", "proportion_total_lipids",
                        "proportion_total_carbs", "proportion_total_protein"]

        # Calcul des moyennes mobiles et des variations en pourcentage pour chaque nutriment
        for col in nutrient_cols:
            rolling_col = f"rolling_avg_{col}"
            food_type_dataframe[rolling_col] = food_type_dataframe.groupby(["user_id", "Type"])[
                col
            ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

            pct_col = f"percentage_change_{col}"
            food_type_dataframe[pct_col] = (
                (food_type_dataframe[col] - food_type_dataframe[rolling_col])
                / food_type_dataframe[rolling_col]
                * 100
            )

        return food_type_dataframe

    def pandas_percentage_change_food_type(self):
        """
        Saves the result of the percentage change computation into the 'data' folder,
        with the output file named 'food_type_percentage_change_pandas.xlsx'.
        """
        result_df = self.compute_food_type_percentage_change_pandas()

        if not os.path.exists("data"):
            os.makedirs("data")

        result_df.to_excel(
            "data/food_type_percentage_change_pandas.xlsx",
            sheet_name="Food Type Percentage Changes",
            index=False,
        )


class DuckDBPercentageChange:
    """
    Class to compute percentage changes in nutrient values compared to the average of the last
    4 occurrences of each food type per user using DuckDB.

    Methods:
        compute_food_type_percentage_change_duckdb: Computes percentage changes for a DataFrame.
        duckdb_percentage_change_food_type: Saves the processed data into an Excel file.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset."""
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel(
                "data/user_food_proportion_duckdb.xlsx",
                sheet_name="Proportion Results",
            )

    def compute_food_type_percentage_change_duckdb(self):
        """
        Computes percentage changes of nutrient values compared to the rolling average of the last
        4 occurrences of each food type per user using DuckDB.

        Returns:
            DataFrame: DataFrame with percentage changes and rolling averages added.
        """
        # Création d'une table temporaire dans DuckDB
        duckdb.sql("CREATE TEMP TABLE IF NOT EXISTS food_type_data AS SELECT * FROM dataframe")

        # Liste des colonnes de nutriments
        nutrient_cols = ["total_calories", "total_lipids", "total_carbs", "total_protein",
                        "proportion_total_calories", "proportion_total_lipids",
                        "proportion_total_carbs", "proportion_total_protein"]

        # Construction de la requête SQL
        select_clauses = []
        window_clauses = []
        pct_change_clauses = []

        for col in nutrient_cols:
            # Moyenne mobile
            window_clause = f"""
                AVG({col}) OVER (
                    PARTITION BY user_id, Type
                    ORDER BY date
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) as rolling_avg_{col}
            """
            window_clauses.append(window_clause)

            # Variation en pourcentage
            pct_change_clause = f"""
                CASE
                    WHEN rolling_avg_{col} = 0 THEN NULL
                    ELSE ({col} - rolling_avg_{col}) / rolling_avg_{col} * 100
                END as percentage_change_{col}
            """
            pct_change_clauses.append(pct_change_clause)

            select_clauses.append(col)

        # Requête SQL complète
        query = f"""
        WITH WindowCalc AS (
            SELECT
                user_id,
                Type,
                date,
                {', '.join(select_clauses)},
                {', '.join(window_clauses)}
            FROM food_type_data
            ORDER BY user_id, Type, date
        )
        SELECT
            *,
            {', '.join(pct_change_clauses)}
        FROM WindowCalc
        """

        # Exécution de la requête et conversion en DataFrame
        result_df = duckdb.sql(query).df()

        # Nettoyage
        duckdb.sql("DROP TABLE IF EXISTS food_type_data")

        return result_df

    def duckdb_percentage_change_food_type(self):
        """
        Saves the result of the percentage change computation into the 'data' folder,
        with the output file named 'food_type_percentage_change_duckdb.xlsx'.
        """
        result_df = self.compute_food_type_percentage_change_duckdb()

        if not os.path.exists("data"):
            os.makedirs("data")

        result_df.to_excel(
            "data/food_type_percentage_change_duckdb.xlsx",
            sheet_name="Food Type Percentage Changes",
            index=False,
        )


if __name__ == "__main__":
    pandas_change = PandasPercentageChange()
    duckdb_change = DuckDBPercentageChange()

    pandas_change.pandas_percentage_change_food_type()
    duckdb_change.duckdb_percentage_change_food_type()
