"""
This module implements user-specific rolling window functions for meal data analysis using both Pandas and DuckDB.

The module focuses on calculating trailing averages of nutritional metrics (calories, proteins, etc.)
for each user over a 4-day period. This allows tracking of individual user's nutritional patterns
and trends over time, helping identify changes in eating habits or potential anomalies.

Key Features:
    - User-specific rolling averages over 4-day periods
    - Dual implementation using both Pandas and DuckDB for performance comparison
    - Automated data loading from S3 and result storage
    - Handles large-scale user data efficiently
    - Maintains data integrity with proper date handling and sorting

Input Data:
    - Expects user-aggregated daily meal data from the group_data transformation step
    - Required columns: user_id, date, and various nutritional metrics

Output:
    - Excel files containing the original data enriched with rolling averages per user
    - Separate files for Pandas and DuckDB implementations for comparison
    - Results are stored both locally and in S3 for further analysis

Dependencies:
    - pandas: For DataFrame operations and Excel I/O
    - duckdb: For SQL-based window function calculations
    - aws_s3.connect_s3: For S3 storage integration

Note:
    This implementation differs from the daily window function as it focuses on individual user
    patterns rather than weekday-based patterns across all users.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

import duckdb
import pandas as pd


class PandasWindowFunction:
    """
    A class implementing user-specific rolling window functions using Pandas.

    This class calculates trailing averages of nutritional metrics for each user over
    a 4-day period. For example, for each user on any given day, it computes the average
    of their nutritional values from the previous 4 days, providing insights into their
    recent eating patterns.

    Attributes:
        s3 (S3Manager): Instance of S3Manager for cloud storage operations
        dataframe (pd.DataFrame): The input DataFrame containing user-specific daily meal data
            Required columns:
                - user_id: Unique identifier for each user
                - date: Date of the meals
                - total_calories: Total calories for the user per day
                - total_proteins: Total proteins for the user per day
                - total_carbohydrates: Total carbohydrates for the user per day
                - total_lipids: Total lipids for the user per day

    Methods:
        compute_user_window_pandas: Computes 4-day rolling averages by user
        pandas_window_daily: Saves the processed data to Excel and S3

    Example:
        >>> window_func = PandasWindowFunction()
        >>> df_with_averages = window_func.compute_user_window_pandas()
        >>> window_func.pandas_window_daily()  # Saves results to Excel and S3
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
        
        Raises:
            FileNotFoundError: If required files cannot be downloaded from S3
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
    A class implementing user-specific rolling window functions using DuckDB.

    This class provides an SQL-based implementation of rolling window calculations using
    DuckDB's window functions. For each user, it calculates trailing averages of nutritional
    metrics over a 4-day period, similar to the Pandas implementation but potentially with
    better performance for larger datasets.

    The implementation uses SQL window functions with PARTITION BY user_id and ORDER BY date
    clauses to efficiently compute rolling averages while maintaining data grouped by user.
    It also employs CTEs (Common Table Expressions) for better query organization and
    readability.

    Attributes:
        s3 (S3Manager): Instance of S3Manager for cloud storage operations
        dataframe (pd.DataFrame): The input DataFrame containing user-specific daily meal data
            Required columns:
                - user_id: Unique identifier for each user
                - date: Date of the meals
                - total_calories: Total calories for the user per day
                - total_lipids: Total lipids for the user per day
                - total_carbs: Total carbohydrates for the user per day
                - total_protein: Total proteins for the user per day

    Methods:
        compute_daily_window_duckdb: Computes 4-day rolling averages using SQL window functions
        duckdb_window_daily: Saves the processed data to Excel and S3

    Example:
        >>> window_func = DuckDBWindowFunction()
        >>> df_with_averages = window_func.compute_daily_window_duckdb()
        >>> window_func.duckdb_window_daily()  # Saves results to Excel and S3
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


def main():
    """
    Main function to compute user-based window functions using both Pandas and DuckDB
    """
    try:
        print("Computing user-based window functions...")
        
        # Initialize aggregators
        pandas_agg = PandasWindowFunction()
        duckdb_agg = DuckDBWindowFunction()
        
        # Compute and save Pandas results
        print("Computing Pandas window function...")
        pandas_agg.pandas_window_daily()
        
        # Compute and save DuckDB results
        print("Computing DuckDB window function...")
        duckdb_agg.duckdb_window_daily()
        
        print("User-based window functions computed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()