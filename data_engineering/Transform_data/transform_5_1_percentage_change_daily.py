"""
This script calculates percentage changes in nutrients for meal data.
The changes are relative to the rolling average of the last 4 same weekdays.
It generates Excel files with the processed data for further analysis.
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
    Class to compute percentage changes in nutrient values compared to the average of the last
    4 same weekdays for the Daily dataset using Pandas.

    Methods:
        compute_daily_percentage_change_pandas: Computes percentage changes for a DataFrame.
        pandas_percentage_change_daily: Saves the processed data into an Excel file.
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
    Class to compute percentage changes in nutrient values compared to the average of the last
    4 same weekdays for the Daily dataset using DuckDB.

    Methods:
        compute_daily_percentage_change_duckdb: Computes percentage changes for a DataFrame.
        duckdb_percentage_change_daily: Saves the processed data into an Excel file.
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


if __name__ == "__main__":
    pandas_change = PandasPercentageChange()
    duckdb_change = DuckDBPercentageChange()

    pandas_change.pandas_percentage_change_daily()
    duckdb_change.duckdb_percentage_change_daily()
