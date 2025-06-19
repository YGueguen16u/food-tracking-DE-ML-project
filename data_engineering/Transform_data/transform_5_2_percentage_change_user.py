# data_engineering/Transform_data/transform_5_2_percentage_change_user.py

"""
This module calculates percentage changes in nutritional values for each user over time.

It analyzes how a user's daily nutrient intake changes compared to their recent history,
using a 4-day rolling average as the baseline. This helps identify trends and variations
in individual dietary patterns.

Key Features:
    - User-specific analysis: Tracks changes for each user independently
    - 4-day rolling window: Compares current intake to recent average
    - Multiple nutrients: Analyzes calories, proteins, lipids, and carbohydrates
    - Dual implementation: Both Pandas and DuckDB for performance comparison
    - S3 integration: Loads data and saves results to AWS S3

The module supports two processing approaches:
1. Pandas: Using DataFrame operations and groupby functions
2. DuckDB: Using SQL window functions and CTEs

Output Format:
    Excel files containing:
    - Original nutritional values
    - 4-day rolling averages per nutrient
    - Percentage changes from rolling averages
    - Results grouped by user_id and date
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

import duckdb
import pandas as pd


class PandasPercentageChange:
    """
    Calculates user-specific nutrient intake changes using Pandas operations.
    
    This class analyzes how each user's nutritional intake changes over time by:
    1. Computing 4-day rolling averages of nutrient intake
    2. Calculating percentage changes from these averages
    3. Organizing results by user and date
    
    The analysis covers multiple nutrients:
    - Calories
    - Proteins
    - Lipids
    - Carbohydrates
    
    Attributes:
        s3 (S3Manager): Manager for AWS S3 operations
        dataframe (pd.DataFrame): Input data containing user nutritional records
    
    Methods:
        compute_user_percentage_change_pandas: Calculates rolling averages and changes
        pandas_percentage_change_daily: Saves results to Excel and uploads to S3
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset."""
        self.s3 = S3Manager()
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            temp_file = "temp_pandas_data.xlsx"
            try:
                if self.s3.download_file("transform/folder_4_windows_function/user/user_daily_window_function_pandas.xlsx", temp_file):
                    self.dataframe = pd.read_excel(temp_file, sheet_name="User Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_user_percentage_change_pandas(self):
        """
        Computes percentage changes in nutrient values using Pandas operations.
        
        For each user, the function:
        1. Sorts data chronologically
        2. Calculates 4-day rolling averages for each nutrient
        3. Computes percentage deviations from these averages
        
        The calculation uses the formula:
        percentage_change = ((current_value - rolling_avg) / rolling_avg) * 100
        
        Returns:
            pd.DataFrame: DataFrame containing:
                - Original columns (user_id, date, nutrient values)
                - Rolling averages (rolling_avg_*)
                - Percentage changes (percentage_change_*)
                Sorted by user_id and date
        """
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])
        user_dataframe = self.dataframe.sort_values(["user_id", "date"])

        # Calculate rolling averages for each user
        for col in ["total_calories", "total_lipids", "total_carbs", "total_protein"]:
            rolling_col = f"rolling_avg_{col}"
            user_dataframe[rolling_col] = user_dataframe.groupby("user_id")[
                col
            ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

            pct_col = f"percentage_change_{col}"
            user_dataframe[pct_col] = (
                (user_dataframe[col] - user_dataframe[rolling_col])
                / user_dataframe[rolling_col]
            ) * 100

        return user_dataframe

    def pandas_percentage_change_daily(self):
        """
        Saves the percentage change computation results to S3.
        
        The results are saved as an Excel file containing:
        - Original nutrient values
        - Rolling averages for each nutrient
        - Percentage changes from the rolling averages
        - Data grouped by user_id and sorted by date
        
        The file is saved to:
        transform/folder_5_percentage_change/user/user_daily_percentage_change_pandas.xlsx
        
        Raises:
            FileNotFoundError: If there are issues accessing S3
            Exception: If there are computation or file handling errors
        """
        temp_file = "temp_percentage_pandas.xlsx"
        try:
            result_df = self.compute_user_percentage_change_pandas()
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="User Daily Aggregation", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_5_percentage_change/user/user_daily_percentage_change_pandas.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


class DuckDBPercentageChange:
    """
    Calculates user-specific nutrient intake changes using DuckDB SQL operations.
    
    This class analyzes how each user's nutritional intake changes over time by:
    1. Computing 4-day rolling averages using SQL window functions
    2. Calculating percentage changes through CTEs
    3. Organizing results by user and date
    
    SQL Implementation:
    - ranked_data CTE: Assigns chronological ranks to each user's records
    - rolling_avg CTE: Computes 4-day moving averages using window functions
    - percentage_change CTE: Calculates deviations from rolling averages
    
    The analysis covers multiple nutrients:
    - Calories
    - Proteins
    - Lipids
    - Carbohydrates
    
    Attributes:
        s3 (S3Manager): Manager for AWS S3 operations
        dataframe (pd.DataFrame): Input data containing user nutritional records
    
    Methods:
        compute_user_percentage_change_duckdb: Executes SQL analysis pipeline
        duckdb_percentage_change_daily: Saves results to Excel and uploads to S3
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset."""
        self.s3 = S3Manager()
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            temp_file = "temp_duckdb_data.xlsx"
            try:
                if self.s3.download_file("transform/folder_4_windows_function/user/user_daily_window_function_duckdb.xlsx", temp_file):
                    self.dataframe = pd.read_excel(temp_file, sheet_name="User Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_user_percentage_change_duckdb(self):
        """
        Computes percentage changes in nutrient values using DuckDB SQL operations.
        
        The function executes a SQL query that:
        1. Ranks each user's records chronologically
        2. Computes 4-day rolling averages using window functions
        3. Calculates percentage deviations using the formula:
           ((current - average) / average) * 100
        
        SQL Implementation Details:
        - Uses CTEs for clear, modular query structure
        - Employs window functions for efficient aggregation
        - Handles all nutrients in a single pass
        
        Returns:
            pd.DataFrame: DataFrame containing:
                - Original columns (user_id, date, nutrient values)
                - Rolling averages (rolling_avg_*)
                - Percentage changes (percentage_change_*)
                Sorted by user_id and date
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("dataframe", self.dataframe)

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
        ),
        percentage_change AS (
            SELECT *,
                   ((total_calories - rolling_avg_total_calories) / rolling_avg_total_calories) * 100 AS percentage_change_total_calories,
                   ((total_lipids - rolling_avg_total_lipids) / rolling_avg_total_lipids) * 100 AS percentage_change_total_lipids,
                   ((total_carbs - rolling_avg_total_carbs) / rolling_avg_total_carbs) * 100 AS percentage_change_total_carbs,
                   ((total_protein - rolling_avg_total_protein) / rolling_avg_total_protein) * 100 AS percentage_change_total_protein
            FROM rolling_avg
        )
        SELECT *
        FROM percentage_change
        ORDER BY user_id, date
        """

        return conn.execute(query).df()

    def duckdb_percentage_change_daily(self):
        """
        Saves the percentage change computation results to S3.
        
        The results are saved as an Excel file containing:
        - Original nutrient values
        - Rolling averages computed via SQL window functions
        - Percentage changes from the rolling averages
        - Data grouped by user_id and sorted by date
        
        The file is saved to:
        transform/folder_5_percentage_change/user/user_daily_percentage_change_duckdb.xlsx
        
        Raises:
            FileNotFoundError: If there are issues accessing S3
            Exception: If there are computation or file handling errors
        """
        temp_file = "temp_percentage_duckdb.xlsx"
        try:
            result_df = self.compute_user_percentage_change_duckdb()
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="User Daily Aggregation", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_5_percentage_change/user/user_daily_percentage_change_duckdb.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


def main():
    """
    Main function to compute user-based percentage changes using both Pandas and DuckDB
    """
    try:
        print("Computing user-based percentage changes...")
        
        # Initialize calculators
        pandas_change = PandasPercentageChange()
        duckdb_change = DuckDBPercentageChange()
        
        # Compute and save Pandas results
        print("Computing Pandas percentage changes...")
        pandas_change.pandas_percentage_change_daily()
        
        # Compute and save DuckDB results
        print("Computing DuckDB percentage changes...")
        duckdb_change.duckdb_percentage_change_daily()
        
        print("User-based percentage changes computed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()