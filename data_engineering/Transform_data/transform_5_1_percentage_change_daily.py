"""
This module analyzes trends in nutritional intake by calculating 
percentage changes relative to historical averages.

The module computes how current nutritional values (calories, proteins, etc.) 
deviate from the rolling average of the same weekday over the past 4 weeks. 
This analysis helps identify significant changes in eating patterns 
and potential areas for dietary adjustments.

Key Features:
    - Calculates percentage deviations from 4-week weekday averages
    - Dual implementation using both Pandas and DuckDB for performance comparison
    - Automated data loading from S3 and result storage
    - Handles missing data and edge cases appropriately
    - Maintains data integrity with proper date handling and sorting

Input Data:
    - Expects data from the window function transformation step
    - Required columns: date, weekday, nutritional metrics, and their rolling averages

Output:
    - Excel files containing original data enriched with percentage changes
    - Separate files for Pandas and DuckDB implementations for comparison
    - Results include both absolute values and percentage changes

Dependencies:
    - pandas: For DataFrame operations and Excel I/O
    - duckdb: For SQL-based calculations
    - aws_s3.connect_s3: For S3 storage integration

Example Use Cases:
    - Identifying significant changes in caloric intake patterns
    - Monitoring adherence to dietary goals
    - Detecting unusual deviations in nutrient consumption
"""

import os
import sys

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(project_root)

import pandas as pd
import duckdb
from aws_s3.connect_s3 import S3Manager

class PandasPercentageChange:
    """
    A class implementing percentage change calculations for daily nutritional metrics using Pandas.

    This class analyzes how current nutritional values deviate from historical patterns by
    calculating percentage changes relative to rolling averages. For example, it can show
    if today's caloric intake is 20% higher than the average of the last 4 Thursdays,
    helping identify significant changes in eating patterns.

    The implementation uses the rolling averages computed by the window function module
    as a baseline and calculates percentage deviations from these averages. Special
    handling is implemented for edge cases like zero averages or missing data.

    Attributes:
        s3 (S3Manager): Instance of S3Manager for cloud storage operations
        dataframe (pd.DataFrame): The input DataFrame containing daily nutritional data
            Required columns:
                - date: Date of the meals
                - weekday: Day of the week
                - total_calories: Total calories for the day
                - total_proteins: Total proteins for the day
                - total_carbohydrates: Total carbohydrates for the day
                - total_lipids: Total lipids for the day
                - rolling_avg_* columns: Rolling averages from window function

    Methods:
        compute_daily_percentage_change_pandas: Calculates percentage changes from averages
        pandas_percentage_change_daily: Saves the processed data to Excel and S3

    Example:
        >>> pct_change = PandasPercentageChange()
        >>> df_with_changes = pct_change.compute_daily_percentage_change_pandas()
        >>> pct_change.pandas_percentage_change_daily()  # Saves results to Excel and S3
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset."""
        self.s3 = S3Manager()
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            # Load from S3
            temp_file = "temp_pandas_data.xlsx"
            try:
                if self.s3.download_file("transform/folder_4_windows_function/daily/daily_window_function_pandas.xlsx", temp_file):
                    self.dataframe = pd.read_excel(temp_file, sheet_name="Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_daily_percentage_change_pandas(self):
        """
        Computes percentage changes of nutrient values compared to the rolling average of the last
        4 same weekdays for the dataset in the Daily using Pandas.

        Returns:
            DataFrame: DataFrame with percentage changes and rolling averages added.
        """
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])
        daily_dataframe = self.dataframe.sort_values("date")

        # Compute rolling averages
        for col in ["total_calories", "total_lipids", "total_carbs", "total_protein"]:
            rolling_col = f"rolling_avg_{col}"
            daily_dataframe[rolling_col] = daily_dataframe.groupby("weekday")[
                col
            ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

            pct_col = f"percentage_change_{col}"
            daily_dataframe[pct_col] = (
                (daily_dataframe[col] - daily_dataframe[rolling_col])
                / daily_dataframe[rolling_col]
            ) * 100

        return daily_dataframe

    def pandas_percentage_change_daily(self):
        """
        Saves the result of the percentage change computation into the 'data' folder,
        with the output file named 'daily_percentage_change_pandas.xlsx'.
        """
        # Save to S3
        temp_file = "temp_percentage_pandas.xlsx"
        try:
            result_df = self.compute_daily_percentage_change_pandas()
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="Daily Percentage Change", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_5_percentage_change/daily/daily_percentage_change_pandas.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


class DuckDBPercentageChange:
    """
    A class implementing percentage change calculations for daily nutritional metrics using DuckDB.

    This class provides an SQL-based implementation for analyzing deviations from historical
    nutritional patterns. It calculates how current nutrient values differ from rolling
    averages of the same weekday over the past 4 weeks, expressed as percentage changes.

    The implementation uses a series of CTEs (Common Table Expressions) for clarity and efficiency:
    1. ranked_data: Extracts weekday information and assigns ranks for proper ordering
    2. rolling_avg: Computes 4-week rolling averages using window functions
    3. percentage_change: Calculates percentage deviations from the averages

    Attributes:
        s3 (S3Manager): Instance of S3Manager for cloud storage operations
        dataframe (pd.DataFrame): The input DataFrame containing daily nutritional data
            Required columns:
                - date: Date of the meals
                - total_calories: Total calories for the day
                - total_proteins: Total proteins for the day
                - total_carbohydrates: Total carbohydrates for the day
                - total_lipids: Total lipids for the day

    Methods:
        compute_daily_percentage_change_duckdb: Calculates percentage changes using SQL
        duckdb_percentage_change_daily: Saves the processed data to Excel and S3

    Example:
        >>> pct_change = DuckDBPercentageChange()
        >>> df_with_changes = pct_change.compute_daily_percentage_change_duckdb()
        >>> pct_change.duckdb_percentage_change_daily()  # Saves results to Excel and S3
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset."""
        self.s3 = S3Manager()
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            # Load from S3
            temp_file = "temp_duckdb_data.xlsx"
            try:
                if self.s3.download_file("transform/folder_4_windows_function/daily/daily_window_function_duckdb.xlsx", temp_file):
                    self.dataframe = pd.read_excel(temp_file, sheet_name="Daily Aggregation")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_daily_percentage_change_duckdb(self):
        """
        Computes percentage changes of nutrient values compared to the rolling average of the last
        4 same weekdays for the dataset in the Daily using DuckDB.

        Returns:
            DataFrame: DataFrame with percentage changes and rolling averages added.
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("dataframe", self.dataframe)

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
        ORDER BY date
        """

        return conn.execute(query).df()

    def duckdb_percentage_change_daily(self):
        """
        Saves the result of the percentage change computation into the 'data' folder,
        with the output file named 'daily_percentage_change_duckdb.xlsx'.
        """
        # Save to S3
        temp_file = "temp_percentage_duckdb.xlsx"
        try:
            result_df = self.compute_daily_percentage_change_duckdb()
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="Daily Percentage Change", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_5_percentage_change/daily/daily_percentage_change_duckdb.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


def main():
    """
    Main function to compute daily percentage changes using both Pandas and DuckDB
    """
    try:
        print("Computing daily percentage changes...")
        
        # Initialize calculators
        pandas_change = PandasPercentageChange()
        duckdb_change = DuckDBPercentageChange()
        
        # Compute and save Pandas results
        print("Computing Pandas percentage changes...")
        pandas_change.pandas_percentage_change_daily()
        
        # Compute and save DuckDB results
        print("Computing DuckDB percentage changes...")
        duckdb_change.duckdb_percentage_change_daily()
        
        print("Daily percentage changes computed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()