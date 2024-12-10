"""
This module calculates rolling averages of meal data using both Pandas and DuckDB.
It calculates the average of the last 4 same weekdays (e.g., Thursdays).
The results are saved as Excel files in the same directory as the input file.
"""

import os

import duckdb
import pandas as pd


class PandasWindowFunction:
    """
    Class to compute a window function in Pandas that calculates the rolling average of the last
    4 same weekdays for a given dataset.

    Methods:
        compute_weekly_window: Computes the rolling average for a specific sheet in an Excel file.
        save_result: Saves the processed data into an Excel file.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset and food type data."""
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel("data/pandas_aggregation_results.xlsx")

    def compute_daily_window_pandas(self):
        """
        Computes the rolling average of the last 4 same weekdays for the dataset
        in the specified sheet.
        Returns:
            DataFrame: DataFrame with the rolling averages added.
        """
        # Ensure the 'date' column is in datetime format
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])

        # Extract the weekday for each date
        self.dataframe["weekday"] = self.dataframe["date"].dt.day_name()

        # Sort by date
        daily_dataframe = self.dataframe.sort_values("date")

        # Apply rolling average grouped by weekday
        for col in ["total_calories", "total_lipids", "total_carbs", "total_protein"]:
            daily_dataframe[f"rolling_avg_{col}"] = daily_dataframe.groupby("weekday")[
                col
            ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

        return daily_dataframe

    def pandas_window_daily(self):
        """
        Saves the result of the weekly window function into the 'data' folder,
        with the output file named 'daily_window_function_pandas.xlsx'.
        """
        # Define the output file path within the 'data' folder
        output_file = os.path.join("data", "daily_window_function_pandas.xlsx")

        # Compute the rolling averages
        result_df = self.compute_daily_window_pandas()

        # Save the result as an Excel file
        with pd.ExcelWriter(output_file, mode="w") as writer:
            result_df.to_excel(writer, sheet_name="Daily Aggregation", index=False)

        print(f"Results saved to: {output_file}")


class DuckDBWindowFunction:
    """
    Class to compute a window function in DuckDB that calculates the rolling average of the last
    4 same weekdays for a given dataset.

    Methods:
        compute_weekly_window: Computes the rolling average for a specific sheet in an Excel file.
        save_result: Saves the processed data into an Excel file.
    """
    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset and food type data."""
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel("data/duckdb_aggregation_results.xlsx")

    def compute_daily_window_duckdb(self):
        """
        Computes the rolling average of the last 4 same weekdays for the dataset
        in the specified sheet using DuckDB.

        Returns:
            DataFrame: DataFrame with the rolling averages added.
        """
        conn = duckdb.connect(database=":memory:")
        daily_dataframe_duckdb = self.dataframe
        # Register the DataFrame as a DuckDB table
        conn.register("dataframe", daily_dataframe_duckdb)

        # DuckDB query for the rolling average
        query = """
        WITH ranked_data AS (
            SELECT *,
                CASE STRFTIME('%w', date)
                   WHEN '0' THEN 'Sunday'
                   WHEN '1' THEN 'Monday'
                   WHEN '2' THEN 'Tuesday'
                   WHEN '3' THEN 'Wednesday'
                   WHEN '4' THEN 'Thursday'
                   WHEN '5' THEN 'Friday'
                   WHEN '6' THEN 'Saturday'
                END AS weekday,
                ROW_NUMBER() OVER (PARTITION BY STRFTIME('%w', date) ORDER BY date) AS weekday_rank
            FROM dataframe
        ),
        rolling_avg AS (
            SELECT date,
                   total_calories,
                   total_lipids,
                   total_carbs,
                   total_protein,
                   weekday,
                   AVG(total_calories) OVER (
                       PARTITION BY weekday
                       ORDER BY weekday_rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_calories,
                   AVG(total_lipids) OVER (
                       PARTITION BY weekday
                       ORDER BY weekday_rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_lipids,
                   AVG(total_carbs) OVER (
                       PARTITION BY weekday
                       ORDER BY weekday_rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_carbs,
                   AVG(total_protein) OVER (
                       PARTITION BY weekday
                       ORDER BY weekday_rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_protein
            FROM ranked_data
        )
        SELECT *
        FROM rolling_avg
        ORDER BY date
        """
        return conn.execute(query).df()

    def duckdb_window_daily(self):
        """
        Saves the result of the weekly window function into the 'data' folder,
        with the output file named 'daily_window_function_duckdb.xlsx'.
        """
        # Define the output file path within the 'data' folder
        output_file = os.path.join("data", "daily_window_function_duckdb.xlsx")

        # Compute the rolling averages
        result_df = self.compute_daily_window_duckdb()

        # Save the result as an Excel file
        with pd.ExcelWriter(output_file, mode="w") as writer:
            result_df.to_excel(writer, sheet_name="Daily Aggregation", index=False)

        print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    pandas_agg = PandasWindowFunction()
    duckdb_agg = DuckDBWindowFunction()

    # Run aggregations and capture results and times
    pandas_agg.pandas_window_daily()

    # DuckDB
    duckdb_agg.duckdb_window_daily()
