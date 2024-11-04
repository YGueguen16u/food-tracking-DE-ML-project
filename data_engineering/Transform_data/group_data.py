"""
This module performs grouping and aggregation of meal data using both Pandas and DuckDB.
It also compares the results and measures the execution time for both methods.
"""

import os
import time

import duckdb
import matplotlib.pyplot as plt
import pandas as pd


class PandasAggregation:
    """
    Class to perform data aggregation using Pandas for each required sheet.

    Methods:
        daily_aggregation: Aggregates daily data and saves to Excel.
        user_daily_aggregation: Aggregates user daily data and saves to Excel.
        daily_mean_per_food: Aggregates daily mean per food and saves to Excel.
        user_food_type_grouping: Aggregates data by user and food type and saves to Excel.
    """

    def __init__(self, dataframe=None, food_type_data=None):
        """Initializes the data by loading the main dataset and food type data."""
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel("data/combined_meal_data_filtered.xlsx")

        if food_type_data is not None:
            self.food_type_data = food_type_data
        else:
            self.food_type_data = pd.read_excel(
                "C:/Users/GUEGUEN/Desktop/WSApp/IM/DB/raw_food_data/aliments.XLSX"
            )

        # Fusion des informations de type d'aliment avec le dataset principal
        self.dataframe = self.dataframe.merge(
            self.food_type_data[["Aliment", "Type"]], on="Aliment", how="left"
        )

    def daily_aggregation(self):
        """
        Aggregates daily data by calculating average nutrients per user per day.

        Returns:
            tuple: Aggregated daily data as DataFrame and execution time.
        """
        # Calculate distinct user count per day
        distinct_users_per_day = (
            self.dataframe.groupby("date")["user_id"].nunique().reset_index()
        )
        distinct_users_per_day.columns = ["date", "distinct_user_count"]

        # Sum nutrients per day and divide by distinct user count for averages
        daily_aggregation = (
            self.dataframe.groupby("date")
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
        print(type(daily_aggregation))
        return daily_aggregation

    def user_daily_aggregation(self):
        """
        Aggregates data by user and day.

        Returns:
            tuple: Aggregated user daily data as DataFrame and execution time.
        """
        # Sum nutrients by user and day
        user_daily_aggregation = (
            self.dataframe.groupby(["date", "user_id"])
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
        print(type(user_daily_aggregation))
        return user_daily_aggregation

    def daily_mean_per_food(self):
        """
        Aggregates daily mean nutrient data per food item.

        Returns:
            tuple: Aggregated daily mean per food as DataFrame and execution time.
        """
        # Calculate mean nutrients per food item per day
        daily_mean_per_food = (
            self.dataframe.groupby(["date", "Aliment"])
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
        return daily_mean_per_food

    def user_food_type_grouping(self):
        """
        Aggregates data by user and food type, calculating averages per food type.

        Returns:
            tuple: Aggregated data by user and food type as DataFrame and execution time.
        """
        # Aggregate nutrients and count food items by type
        user_food_type_grouping = (
            self.dataframe.groupby(["user_id", "Type"])
            .agg(
                total_calories=("total_calories", "sum"),
                total_lipids=("total_lipids", "sum"),
                total_carbs=("total_carbs", "sum"),
                total_protein=("total_protein", "sum"),
                food_count=("Aliment", "count"),
            )
            .reset_index()
        )
        # Calculate average values per food item type
        user_food_type_grouping["avg_calories_per_type"] = (
            user_food_type_grouping["total_calories"]
            / user_food_type_grouping["food_count"]
        )
        user_food_type_grouping["avg_lipids_per_type"] = (
            user_food_type_grouping["total_lipids"]
            / user_food_type_grouping["food_count"]
        )
        user_food_type_grouping["avg_carbs_per_type"] = (
            user_food_type_grouping["total_carbs"]
            / user_food_type_grouping["food_count"]
        )
        user_food_type_grouping["avg_protein_per_type"] = (
            user_food_type_grouping["total_protein"]
            / user_food_type_grouping["food_count"]
        )
        return user_food_type_grouping

    def pandas_aggregations(self):
        """
        Aggregates all data and writes to a single Excel file, returning results and elapsed time.

        Returns:
            dict: A dictionary containing each aggregation result and the total elapsed time.
        """
        start_time = time.time()  # Start timing the entire process

        # Run all aggregation methods and capture their results
        daily_agg = self.daily_aggregation()
        user_daily_agg = self.user_daily_aggregation()
        daily_mean_food = self.daily_mean_per_food()
        user_food_type_agg = self.user_food_type_grouping()

        if not os.path.exists("data"):
            os.makedirs("data")

        # Write all results to Excel in one place
        with pd.ExcelWriter("data/pandas_aggregation_results.xlsx", mode="w") as writer:
            daily_agg.to_excel(writer, sheet_name="Daily Aggregation", index=False)
            user_daily_agg.to_excel(
                writer, sheet_name="User Daily Aggregation", index=False
            )
            daily_mean_food.to_excel(
                writer, sheet_name="Daily Mean Per Food", index=False
            )
            user_food_type_agg.to_excel(
                writer, sheet_name="User Food Type Grouping", index=False
            )

        # Capture the total elapsed time
        elapsed_time = time.time() - start_time
        print(
            f"All aggregations written to data/pandas_aggregation_results.xlsx in {elapsed_time:.2f} seconds"
        )

        # Return results and elapsed time in a dictionary
        return {
            "Daily Aggregation": daily_agg,
            "User Daily Aggregation": user_daily_agg,
            "Daily Mean Per Food": daily_mean_food,
            "User Food Type Grouping": user_food_type_agg,
            "Elapsed Time": elapsed_time,
        }


class DuckDBAggregation:
    """
    Class to perform data aggregation using DuckDB for each required sheet.

    Methods:
        daily_aggregation: Aggregates daily data and saves to Excel.
        user_daily_aggregation: Aggregates user daily data and saves to Excel.
        daily_mean_per_food: Aggregates daily mean per food and saves to Excel.
        user_food_type_grouping: Aggregates data by user and food type and saves to Excel.
    """

    def __init__(self, dataframe=None, food_type_data=None):
        """Initializes the data by loading the main dataset and food type data and sets up DuckDB connection."""
        self.conn = duckdb.connect(database=":memory:")

        # Charger et fusionner les données dans DuckDB
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel("data/combined_meal_data_filtered.xlsx")

        if food_type_data is not None:
            self.food_type_data = food_type_data
        else:
            self.food_type_data = pd.read_excel(
                "C:/Users/GUEGUEN/Desktop/WSApp/IM/DB/raw_food_data/aliments.XLSX"
            )

        # Fusion des informations de type d'aliment avec le dataset principal
        self.dataframe = self.dataframe.merge(
            self.food_type_data[["Aliment", "Type"]], on="Aliment", how="left"
        )

        # Enregistrement des DataFrames comme tables DuckDB
        self.conn.register("dataframe", self.dataframe)
        self.conn.register("food_type_data", self.food_type_data)
        print("DuckDB initialized and data registered successfully.")

    def daily_aggregation(self):
        """
        Aggregates daily data by calculating average nutrients per user per day,
        using column names consistent with the Pandas class.

        Returns:
            tuple: Aggregated daily data as DataFrame and execution time.
        """
        daily_aggregation = self.conn.execute(
            """
            SELECT date,
                   SUM(total_calories) / COUNT(DISTINCT user_id) AS total_calories,
                   SUM(total_lipids) / COUNT(DISTINCT user_id) AS total_lipids,
                   SUM(total_carbs) / COUNT(DISTINCT user_id) AS total_carbs,
                   SUM(total_protein) / COUNT(DISTINCT user_id) AS total_protein,
                   COUNT(DISTINCT user_id) AS distinct_user_count
            FROM dataframe
            GROUP BY date
            ORDER BY date
            """
        ).df()

        return daily_aggregation

    def user_daily_aggregation(self):
        """
        Aggregates data by user and day, with standardized column names.

        Returns:
            tuple: Aggregated user daily data as DataFrame and execution time.
        """
        user_daily_aggregation = self.conn.execute(
            """
            SELECT date,
                   user_id,
                   SUM(total_calories) AS total_calories,
                   SUM(total_lipids) AS total_lipids,
                   SUM(total_carbs) AS total_carbs,
                   SUM(total_protein) AS total_protein
            FROM dataframe
            GROUP BY date, user_id
            ORDER BY date
            """
        ).df()
        return user_daily_aggregation

    def daily_mean_per_food(self):
        """
        Aggregates daily mean nutrient data per food item, with consistent column names.

        Returns:
            tuple: Aggregated daily mean per food as DataFrame and execution time.
        """
        daily_mean_per_food = self.conn.execute(
            """
            SELECT date,
                   Aliment,
                   AVG(total_calories) AS total_calories,
                   AVG(total_lipids) AS total_lipids,
                   AVG(total_carbs) AS total_carbs,
                   AVG(total_protein) AS total_protein
            FROM dataframe
            GROUP BY date, Aliment
            ORDER BY date, Aliment
            """
        ).df()

        return daily_mean_per_food

    def user_food_type_grouping(self):
        """
        Aggregates data by user and food type, calculating averages per food type,
        with standardized column names for compatibility.

        Returns:
            tuple: Aggregated data by user and food type as DataFrame and execution time.
        """
        user_food_type_grouping = self.conn.execute(
            """
            SELECT d.user_id,
                   f.Type,
                   SUM(d.total_calories) AS total_calories,
                   SUM(d.total_lipids) AS total_lipids,
                   SUM(d.total_carbs) AS total_carbs,
                   SUM(d.total_protein) AS total_protein,
                   COUNT(d.Aliment) AS food_count,
                   SUM(d.total_calories) / COUNT(d.Aliment) AS avg_calories_per_type,
                   SUM(d.total_lipids) / COUNT(d.Aliment) AS avg_lipids_per_type,
                   SUM(d.total_carbs) / COUNT(d.Aliment) AS avg_carbs_per_type,
                   SUM(d.total_protein) / COUNT(d.Aliment) AS avg_protein_per_type
            FROM dataframe AS d
            LEFT JOIN food_type_data AS f ON d.Aliment = f.Aliment
            GROUP BY d.user_id, f.Type
            ORDER BY d.user_id, f.Type
            """
        ).df()

        # Renaming the averaged columns to be consistent with Pandas
        user_food_type_grouping = user_food_type_grouping.rename(
            columns={
                "avg_calories_per_type": "total_calories",
                "avg_lipids_per_type": "total_lipids",
                "avg_carbs_per_type": "total_carbs",
                "avg_protein_per_type": "total_protein",
            }
        )
        return user_food_type_grouping

    def duckdb_aggregations(self):
        """
        Performs all data aggregations using DuckDB, writes results to an Excel file, and returns execution time.

        Returns:
            dict: A dictionary containing each aggregation result and the total elapsed time.
        """
        start_time = time.time()  # Start timing the entire process

        # Run all aggregation methods and capture their results
        daily_agg = self.daily_aggregation()
        user_daily_agg = self.user_daily_aggregation()
        daily_mean_food = self.daily_mean_per_food()
        user_food_type_agg = self.user_food_type_grouping()

        if not os.path.exists("data"):
            os.makedirs("data")

        # Write results to Excel file with separate sheets
        with pd.ExcelWriter("data/duckdb_aggregation_results.xlsx", mode="w") as writer:
            daily_agg.to_excel(writer, sheet_name="Daily Aggregation", index=False)
            user_daily_agg.to_excel(
                writer, sheet_name="User Daily Aggregation", index=False
            )
            daily_mean_food.to_excel(
                writer, sheet_name="Daily Mean Per Food", index=False
            )
            user_food_type_agg.to_excel(
                writer, sheet_name="User Food Type Grouping", index=False
            )

        # Capture the total elapsed time
        elapsed_time = time.time() - start_time
        print(
            f"All aggregations written to data/duckdb_aggregation_results.xlsx in {elapsed_time:.2f} seconds"
        )

        # Return results and elapsed time in a dictionary
        return {
            "Daily Aggregation": daily_agg,
            "User Daily Aggregation": user_daily_agg,
            "Daily Mean Per Food": daily_mean_food,
            "User Food Type Grouping": user_food_type_agg,
            "Elapsed Time": elapsed_time,
        }


class Comparison:
    """
    Class to compare results between Pandas and DuckDB aggregations.

    Methods:
        compare_data: Compares data by checking for discrepancies in merging.
        compare_execution_times: Compares execution times and returns the faster method.
    """

    @staticmethod
    def compare_execution_times(pandas_time, duckdb_time):
        """
        Compares execution times of Pandas and DuckDB methods.

        Args:
            pandas_time (float): Execution time of Pandas aggregation.
            duckdb_time (float): Execution time of DuckDB aggregation.

        Returns:
            str: Name of the faster method.
        """
        return "Pandas" if pandas_time < duckdb_time else "DuckDB"


if __name__ == "__main__":
    pandas_agg = PandasAggregation()
    duckdb_agg = DuckDBAggregation()
    compare = Comparison()

    # Run aggregations and capture results and times
    pandas_results = pandas_agg.pandas_aggregations()
    duckdb_results = duckdb_agg.duckdb_aggregations()

    # Compare the total elapsed time
    fastest = compare.compare_execution_times(
        pandas_results["Elapsed Time"], duckdb_results["Elapsed Time"]
    )
    print(f"The fastest method is: {fastest}")
