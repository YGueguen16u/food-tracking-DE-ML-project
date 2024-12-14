"""
Calculates 4-day rolling averages of meal data grouped by user_id.
It supports both Pandas and DuckDB implementations for flexibility and performance.
The results are saved as Excel files in the 'data' directory.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

import duckdb
import pandas as pd


class PandasWindowFunction:
    """
    Class to compute a window function in Pandas that calculates the rolling average of the last
    4 days for the Daily dataset grouped by user_id.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset and food type data."""
        self.s3 = S3Manager()
        
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            # Load from S3
            temp_file = "temp_pandas_data.xlsx"
            try:
                if self.s3.download_file("transform/folder_3_group_data/pandas_aggregation_results.xlsx", temp_file):
                    self.dataframe = pd.read_excel(temp_file, sheet_name="User Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_user_window_pandas(self):
        """
        Computes the rolling average of the last 4 days for the dataset grouped by user_id
        in the Daily dataset using Pandas.
        Returns:
            DataFrame: DataFrame with the rolling averages added.
        """
        # Ensure the 'date' column is in datetime format
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])

        # Sort by user_id and date
        user_dataframe = self.dataframe.sort_values(["user_id", "date"])

        # Apply rolling average grouped by user_id
        for col in ["total_calories", "total_lipids", "total_carbs", "total_protein"]:
            user_dataframe[f"rolling_avg_{col}"] = user_dataframe.groupby("user_id")[
                col
            ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

        return user_dataframe

    def pandas_window_daily(self):
        """
        Saves the result of the user_id-based rolling window function into an Excel file.
        """
        # Compute the rolling averages
        result_df = self.compute_user_window_pandas()

        # Save to S3
        temp_file = "temp_window_pandas.xlsx"
        try:
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="User Daily Aggregation", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_4_windows_function/user/user_daily_window_function_pandas.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


class DuckDBWindowFunction:
    """
    Class to compute a window function in DuckDB that calculates the rolling average of the last
    4 days for the Daily dataset grouped by user_id.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset and food type data."""
        self.s3 = S3Manager()
        
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            # Load from S3
            temp_file = "temp_duckdb_data.xlsx"
            try:
                if self.s3.download_file("transform/folder_3_group_data/duckdb_aggregation_results.xlsx", temp_file):
                    self.dataframe = pd.read_excel(temp_file, sheet_name="User Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_daily_window_duckdb(self):
        """
        Computes the rolling average of the last 4 days for the dataset grouped by user_id
        in the Daily dataset using DuckDB.

        Returns:
            DataFrame: DataFrame with the rolling averages added.
        """
        conn = duckdb.connect(database=":memory:")
        user_daily_dataframe_duckdb = self.dataframe
        # Register the DataFrame as a DuckDB table
        conn.register("dataframe", user_daily_dataframe_duckdb)

        # DuckDB query for the rolling average
        query = """
        WITH ranked_data AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date) AS rank
            FROM dataframe
        ),
        rolling_avg AS (
            SELECT *,
                   AVG(total_calories) OVER (
                       PARTITION BY user_id
                       ORDER BY rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_calories,
                   AVG(total_lipids) OVER (
                       PARTITION BY user_id
                       ORDER BY rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_lipids,
                   AVG(total_carbs) OVER (
                       PARTITION BY user_id
                       ORDER BY rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_carbs,
                   AVG(total_protein) OVER (
                       PARTITION BY user_id
                       ORDER BY rank
                       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                   ) AS rolling_avg_total_protein
            FROM ranked_data
        )
        SELECT *
        FROM rolling_avg
        ORDER BY user_id, date
        """
        return conn.execute(query).df()

    def duckdb_window_daily(self):
        """
        Saves the result of the user_id-based rolling window function into an Excel file.
        """
        # Compute the rolling averages
        result_df = self.compute_daily_window_duckdb()

        # Save to S3
        temp_file = "temp_window_duckdb.xlsx"
        try:
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="User Daily Aggregation", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_4_windows_function/user/user_daily_window_function_duckdb.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


if __name__ == "__main__":
    pandas_agg = PandasWindowFunction()
    duckdb_agg = DuckDBWindowFunction()

    # Run aggregations and capture results
    pandas_agg.pandas_window_daily()
    duckdb_agg.duckdb_window_daily()
