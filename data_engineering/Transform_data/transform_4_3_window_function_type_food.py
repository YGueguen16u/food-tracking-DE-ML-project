"""
This module analyzes nutritional composition by food type for each user in the meal tracking system.

The module calculates the proportions of different nutrients (calories, proteins, lipids,
carbohydrates) contributed by each food type to a user's total intake. This analysis helps
understand users' dietary patterns and preferences in terms of food choices.

Key Features:
    - Calculates nutrient proportions by food type for each user
    - Dual implementation using both Pandas and DuckDB for performance comparison
    - Automated data loading from S3 and result storage
    - Handles aggregation and proportion calculations efficiently
    - Maintains data integrity with proper grouping and calculations

Input Data:
    - Expects user and food type aggregated data from the group_data transformation step
    - Required columns: user_id, Type (food type), and various nutritional metrics

Output:
    - Excel files containing proportional analysis of nutrients by food type
    - Separate files for Pandas and DuckDB implementations for comparison
    - Results include percentage contributions of each food type to total nutrients

Dependencies:
    - pandas: For DataFrame operations and Excel I/O
    - duckdb: For SQL-based aggregation calculations
    - aws_s3.connect_s3: For S3 storage integration

Example Use Cases:
    - Understanding which food types contribute most to a user's protein intake
    - Analyzing caloric distribution across different food categories
    - Identifying patterns in users' food type preferences
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

import duckdb
import pandas as pd


class PandasProportionCalculation:
    """
    A class implementing food type proportion calculations using Pandas.

    This class analyzes the nutritional composition of users' diets by calculating
    the proportion of nutrients (calories, proteins, lipids, carbohydrates) that
    comes from each food type. For example, it can show what percentage of a user's
    protein intake comes from meat vs. dairy vs. vegetables.

    The implementation uses Pandas groupby operations to first aggregate nutrients
    by user and food type, then calculates proportions relative to each user's
    total intake.

    Attributes:
        s3 (S3Manager): Instance of S3Manager for cloud storage operations
        dataframe (pd.DataFrame): The input DataFrame containing user and food type data
            Required columns:
                - user_id: Unique identifier for each user
                - Type: Food type category
                - total_calories: Total calories for this food type
                - total_proteins: Total proteins for this food type
                - total_carbohydrates: Total carbohydrates for this food type
                - total_lipids: Total lipids for this food type

    Methods:
        compute_proportions_pandas: Calculates nutrient proportions by food type
        pandas_proportion_results: Saves the processed data to Excel and S3

    Example:
        >>> prop_calc = PandasProportionCalculation()
        >>> df_with_proportions = prop_calc.compute_proportions_pandas()
        >>> prop_calc.pandas_proportion_results()  # Saves results to Excel and S3
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
    A class implementing food type proportion calculations using DuckDB.

    This class provides an SQL-based implementation for analyzing nutritional composition
    by food type using DuckDB's efficient query engine. It calculates what proportion of
    each user's total nutrient intake comes from different food types (e.g., what percentage
    of total calories comes from vegetables vs. proteins).

    The implementation uses SQL Common Table Expressions (CTEs) for clarity and efficiency:
    1. First CTE 'grouped' aggregates nutrients by user and food type
    2. Second CTE 'user_totals' calculates total nutrients per user
    3. Final query joins these to compute proportions with proper handling of floating-point division

    Attributes:
        s3 (S3Manager): Instance of S3Manager for cloud storage operations
        dataframe (pd.DataFrame): The input DataFrame containing user and food type data
            Required columns:
                - user_id: Unique identifier for each user
                - Type: Food type category
                - total_calories: Total calories for this food type
                - total_proteins: Total proteins for this food type
                - total_carbohydrates: Total carbohydrates for this food type
                - total_lipids: Total lipids for this food type

    Methods:
        compute_proportions_duckdb: Calculates nutrient proportions using SQL
        duckdb_proportion_results: Saves the processed data to Excel and S3

    Example:
        >>> prop_calc = DuckDBProportionCalculation()
        >>> df_with_proportions = prop_calc.compute_proportions_duckdb()
        >>> prop_calc.duckdb_proportion_results()  # Saves results to Excel and S3
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