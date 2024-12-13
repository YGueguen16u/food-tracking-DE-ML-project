"""
Calculates 4-day rolling averages of meal data grouped by user_id.
It supports both Pandas and DuckDB implementations for flexibility and performance.
The results are saved as Excel files in the 'data' directory.
"""

import os

import duckdb
import pandas as pd


class PandasWindowFunction:
    """
    Class to compute a window function in Pandas that calculates the rolling average of the last
    4 days for the Daily dataset grouped by user_id.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset and food type data."""
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel(
                "data/pandas_aggregation_results.xlsx",
                sheet_name="User Daily Aggregation",
            )

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
        output_file = os.path.join("data", "user_daily_window_function_pandas.xlsx")

        # Compute the rolling averages
        result_df = self.compute_user_window_pandas()

        with pd.ExcelWriter(output_file, mode="w") as writer:
            result_df.to_excel(writer, sheet_name="User Daily Aggregation", index=False)

        print(f"Results saved to: {output_file}")


class DuckDBWindowFunction:
    """
    Class to compute a window function in DuckDB that calculates the rolling average of the last
    4 days for the Daily dataset grouped by user_id.
    """

    def __init__(self, dataframe=None):
        """Initializes the data by loading the main dataset and food type data."""
        if dataframe is not None:
            self.dataframe = dataframe
        else:
            self.dataframe = pd.read_excel(
                "data/duckdb_aggregation_results.xlsx",
                sheet_name="User Daily Aggregation",
            )

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
        # Define the output file path within the 'data' folder
        output_file = os.path.join("data", "user_daily_window_function_duckdb.xlsx")

        # Compute the rolling averages
        result_df = self.compute_daily_window_duckdb()

        # Save the result as an Excel file
        with pd.ExcelWriter(output_file, mode="w") as writer:
            result_df.to_excel(writer, sheet_name="User Daily Aggregation", index=False)

        print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    pandas_agg = PandasWindowFunction()
    duckdb_agg = DuckDBWindowFunction()

    # Run aggregations and capture results
    pandas_agg.pandas_window_daily()
    duckdb_agg.duckdb_window_daily()
