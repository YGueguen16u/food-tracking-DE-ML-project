"""
Computes nutrient proportions per food type for each user.
Supports Pandas and DuckDB implementations.
Saves results as Excel files in the 'data' directory.
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

import duckdb
import pandas as pd


class PandasProportionCalculation:
    """
    Class to compute proportions of total calories, lipids, proteins, and carbs
    per user and food type using Pandas.
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
                    self.dataframe = pd.read_excel(temp_file, sheet_name="User Food Type Grouping")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_proportions_pandas(self):
        """
        Computes the proportions of total calories, lipids, proteins, and carbs
        per user and food type.
        Returns:
            DataFrame: DataFrame with the proportions added.
        """
        # Group data by user_id and food type, summing nutrient values
        food_type_dataframe = self.dataframe.groupby(
            ["user_id", "Type"], as_index=False
        ).sum()

        # Calculate total nutrients per user
        user_totals = food_type_dataframe.groupby("user_id")[
            ["total_calories", "total_lipids", "total_protein", "total_carbs"]
        ].transform("sum")

        # Calculate proportions
        for col in ["total_calories", "total_lipids", "total_protein", "total_carbs"]:
            food_type_dataframe[f"proportion_{col}"] = (
                food_type_dataframe[col] / user_totals[col]
            )

        return food_type_dataframe

    def pandas_proportion_results(self):
        """
        Saves the result of the proportion calculation into an Excel file.
        """
        # Save to S3
        temp_file = "temp_window_pandas.xlsx"
        try:
            result_df = self.compute_proportions_pandas()
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="Proportion Results", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_4_windows_function/type_food/user_food_proportion_pandas.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


class DuckDBProportionCalculation:
    """
    Class to compute proportions of total calories, lipids, proteins, and carbs
    per user and food type using DuckDB.
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
                    self.dataframe = pd.read_excel(temp_file, sheet_name="User Food Type Grouping")
                else:
                    raise FileNotFoundError("Could not download data from S3")
            finally:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    def compute_proportions_duckdb(self):
        """
        Computes the proportions of total calories, lipids, proteins, and carbs
        per user and food type using DuckDB.
        Returns:
            DataFrame: DataFrame with the proportions added.
        """
        conn = duckdb.connect(database=":memory:")
        conn.register("dataframe", self.dataframe)

        query = """
        WITH grouped AS (
            SELECT user_id, Type,
                   SUM(total_calories) AS total_calories,
                   SUM(total_lipids) AS total_lipids,
                   SUM(total_protein) AS total_protein,
                   SUM(total_carbs) AS total_carbs
            FROM dataframe
            GROUP BY user_id, Type
        ),
        user_totals AS (
            SELECT user_id,
                   SUM(total_calories) AS total_calories_user,
                   SUM(total_lipids) AS total_lipids_user,
                   SUM(total_protein) AS total_protein_user,
                   SUM(total_carbs) AS total_carbs_user
            FROM grouped
            GROUP BY user_id
        )
        SELECT g.user_id, g.Type, g.total_calories, g.total_lipids, g.total_protein, g.total_carbs,
               g.total_calories * 1.0 / t.total_calories_user AS proportion_total_calories,
               g.total_lipids * 1.0 / t.total_lipids_user AS proportion_total_lipids,
               g.total_protein * 1.0 / t.total_protein_user AS proportion_total_protein,
               g.total_carbs * 1.0 / t.total_carbs_user AS proportion_total_carbs
        FROM grouped g
        JOIN user_totals t
        ON g.user_id = t.user_id
        ORDER BY g.user_id, g.Type

        """
        return conn.execute(query).df()

    def duckdb_proportion_results(self):
        """
        Saves the result of the proportion calculation into an Excel file.
        """
        # Save to S3
        temp_file = "temp_window_duckdb.xlsx"
        try:
            result_df = self.compute_proportions_duckdb()
            with pd.ExcelWriter(temp_file, mode="w") as writer:
                result_df.to_excel(writer, sheet_name="Proportion Results", index=False)
            
            if self.s3.upload_file(temp_file, "transform/folder_4_windows_function/type_food/user_food_proportion_duckdb.xlsx"):
                print("Results saved to S3")
            else:
                print("Failed to upload results to S3")
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)


def main():
    """
    Main function to compute food type proportions using both Pandas and DuckDB
    """
    try:
        print("Computing food type proportions...")
        
        # Initialize calculators
        pandas_calc = PandasProportionCalculation()
        duckdb_calc = DuckDBProportionCalculation()
        
        # Compute and save Pandas results
        print("Computing Pandas proportions...")
        pandas_calc.pandas_proportion_results()
        
        # Compute and save DuckDB results
        print("Computing DuckDB proportions...")
        duckdb_calc.duckdb_proportion_results()
        
        print("Food type proportions computed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()