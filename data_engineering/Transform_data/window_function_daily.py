"""
This module calculates rolling averages of meal data using both Pandas and DuckDB.
It processes grouped data to compute the average for the last 4 same weekdays (e.g., last 4 Thursdays).
The results are saved as Excel files in the same directory as the input file.
"""

import os
import time

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

    @staticmethod
    def compute_weekly_window(input_file, sheet_name):
        """
        Computes the rolling average of the last 4 same weekdays for the dataset
        in the specified sheet.

        Args:
            input_file (str): Path to the input Excel file.
            sheet_name (str): Name of the sheet to process.

        Returns:
            DataFrame: DataFrame with the rolling averages added.
        """
        # Load the sheet into a DataFrame
        df = pd.read_excel(input_file, sheet_name=sheet_name)

        # Ensure the 'date' column is in datetime format
        df['date'] = pd.to_datetime(df['date'])

        # Extract the weekday for each date
        df['weekday'] = df['date'].dt.day_name()

        # Sort by date
        df = df.sort_values('date')

        # Apply rolling average grouped by weekday
        for col in ['total_calories', 'total_lipids', 'total_carbs', 'total_protein']:
            df[f'rolling_avg_{col}'] = (
                df.groupby('weekday')[col]
                .transform(lambda x: x.rolling(window=4, min_periods=1).mean())
            )

        return df

    @staticmethod
    def save_result(input_file, sheet_name):
        """
        Saves the result of the weekly window function into the same folder as the input file,
        with the output file named 'daily_window_function_pandas.xlsx'.

        Args:
            input_file (str): Path to the input Excel file.
            sheet_name (str): Name of the sheet to process.
        """
        output_file = os.path.join(os.path.dirname(input_file), "daily_window_function_pandas.xlsx")
        result_df = PandasWindowFunction.compute_weekly_window(input_file, sheet_name)

        with pd.ExcelWriter(output_file, mode="w") as writer:
            result_df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Results saved to: {output_file}")


class DuckDBWindowFunction:
    """
    Class to compute a window function in DuckDB that calculates the rolling average of the last
    4 same weekdays for a given dataset.

    Methods:
        compute_weekly_window: Computes the rolling average for a specific sheet in an Excel file.
        save_result: Saves the processed data into an Excel file.
    """

    @staticmethod
    def compute_weekly_window(input_file, sheet_name):
        """
        Computes the rolling average of the last 4 same weekdays for the dataset
        in the specified sheet using DuckDB.

        Args:
            input_file (str): Path to the input Excel file.
            sheet_name (str): Name of the sheet to process.

        Returns:
            DataFrame: DataFrame with the rolling averages added.
        """
        conn = duckdb.connect(database=":memory:")

        # Load the sheet into a DataFrame
        df = pd.read_excel(input_file, sheet_name=sheet_name)

        # Register the DataFrame as a DuckDB table
        conn.register("dataframe", df)

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

    @staticmethod
    def save_result(input_file, sheet_name):
        """
        Saves the result of the weekly window function into the same folder as the input file,
        with the output file named 'daily_window_function_duckdb.xlsx'.

        Args:
            input_file (str): Path to the input Excel file.
            sheet_name (str): Name of the sheet to process.
        """
        output_file = os.path.join(os.path.dirname(input_file), "daily_window_function_duckdb.xlsx")
        result_df = DuckDBWindowFunction.compute_weekly_window(input_file, sheet_name)

        with pd.ExcelWriter(output_file, mode="w") as writer:
            result_df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    input_file = "data/pandas_aggregation_results.xlsx"
    sheet_name = "Daily Aggregation"

    # Pandas
    PandasWindowFunction.save_result(input_file, sheet_name)

    # DuckDB
    duckdb_input_file = "data/duckdb_aggregation_results.xlsx"
    DuckDBWindowFunction.save_result(duckdb_input_file, sheet_name)
