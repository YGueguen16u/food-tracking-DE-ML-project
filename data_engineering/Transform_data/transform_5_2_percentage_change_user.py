"""
This script calculates percentage changes in nutrients for meal data grouped by user_id.
The changes use the rolling average of the last 4 days per user.
It generates Excel files with the processed data for further analysis.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

import duckdb
import pandas as pd


class PandasPercentageChange:
    """
    Class to compute percentage changes in nutrient values compared to the average of the last
    4 days grouped by user_id using Pandas.

    Methods:
        compute_user_percentage_change_pandas: Computes percentage changes for a DataFrame.
        pandas_percentage_change_daily: Saves the processed data into an Excel file.
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
        Computes percentage changes of nutrient values compared to the rolling average of the last
        4 days for the dataset grouped by user_id using Pandas.

        Returns:
            DataFrame: DataFrame with percentage changes and rolling averages added.
        """
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])
        user_dataframe = self.dataframe.sort_values(["user_id", "date"])

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
        Saves the result of the percentage change computation into the 'data' folder,
        with the output file named 'user_daily_percentage_change_pandas.xlsx'.
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
    Class to compute percentage changes in nutrient values compared to the average of the last
    4 days grouped by user_id using DuckDB.

    Methods:
        compute_user_percentage_change_duckdb: Computes percentage changes for a DataFrame.
        duckdb_percentage_change_daily: Saves the processed data into an Excel file.
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
        Computes percentage changes of nutrient values compared to the rolling average of the last
        4 days for the dataset grouped by user_id using DuckDB.

        Returns:
            DataFrame: DataFrame with percentage changes and rolling averages added.
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
        Saves the result of the percentage change computation into the 'data' folder,
        with the output file named 'user_daily_percentage_change_duckdb.xlsx'.
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