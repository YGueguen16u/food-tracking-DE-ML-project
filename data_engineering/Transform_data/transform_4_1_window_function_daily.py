"""
This module calculates rolling averages of meal data using both Pandas and DuckDB.
It calculates the average of the last 4 same weekdays (e.g., Thursdays).
The results are saved as Excel files in the same directory as the input file.
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
    4 same weekdays for the Daily dataset.

    Methods:
        compute_daily_window_pandas: Computes rolling averages for a DataFrame.
        pandas_window_daily: Saves the processed data into an Excel file.
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
                    self.dataframe = pd.read_excel(temp_file, sheet_name="Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_daily_window_pandas(self):
        """
        Computes the rolling average of the last 4 same weekdays for the dataset
        in the Daily using Pandas.
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
            daily_dataframe[f"rolling_avg_{col}"] = daily_dataframe.groupby("weekday")[col].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

        return daily_dataframe

    def pandas_window_daily(self):
        """
        Saves the result of the weekly window function into the 'data' folder,
        with the output file named 'daily_window_function_pandas.xlsx'.
        """
        # Compute the rolling averages
        result_df = self.compute_daily_window_pandas()

        # Save to S3
        temp_file = "temp_window_pandas.xlsx"
        try:
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="Daily Aggregation", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_4_windows_function/daily/daily_window_function_pandas.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


class DuckDBWindowFunction:
    """
    Class to compute a window function in DuckDB that calculates the rolling average of the last
    4 same weekdays for the Daily dataset.

    Methods:
        compute_daily_window_pandas: Computes rolling averages for a DataFrame.
        duckdb_window_daily: Saves the processed data into an Excel file.
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
                    self.dataframe = pd.read_excel(temp_file, sheet_name="Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_daily_window_duckdb(self):
        """
        Computes the rolling average of the last 4 same weekdays for the dataset
        in the sheet Daily using DuckDB.

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
        # Compute the rolling averages
        result_df = self.compute_daily_window_duckdb()

        # Save to S3
        temp_file = "temp_window_duckdb.xlsx"
        try:
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="Daily Aggregation", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_4_windows_function/daily/daily_window_function_duckdb.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


def main():
    """
    Main function to compute daily window functions using both Pandas and DuckDB
    """
    try:
        print("Computing daily window functions...")
        
        # Initialize aggregators
        pandas_agg = PandasWindowFunction()
        duckdb_agg = DuckDBWindowFunction()
        
        # Compute and save Pandas results
        print("Computing Pandas window function...")
        pandas_agg.pandas_window_daily()
        
        # Compute and save DuckDB results
        print("Computing DuckDB window function...")
        duckdb_agg.duckdb_window_daily()
        
        print("Daily window functions computed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()