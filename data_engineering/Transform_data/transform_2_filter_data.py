"""
This module contains functions for cleaning and filtering meal data.
It also handles S3 operations for input and output data.
"""

import os
import sys
import pandas as pd
from typing import Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

def load_from_s3() -> Optional[pd.DataFrame]:
    """
    Load the combined meal data from S3.
    
    Returns:
        Optional[pd.DataFrame]: Combined meal data or None if error occurs
    """
    s3 = S3Manager()
    input_key = "transform/folder_1_combine/combined_meal_data.xlsx"
    temp_input = "temp_input.xlsx"
    
    try:
        if s3.download_file(input_key, temp_input):
            return pd.read_excel(temp_input)
        return None
    except Exception as e:
        print(f"Error loading data from S3: {str(e)}")
        return None
    finally:
        if os.path.exists(temp_input):
            os.remove(temp_input)

def save_to_s3(df: pd.DataFrame) -> bool:
    """
    Save the filtered DataFrame to S3.
    
    Args:
        df: Filtered DataFrame to save
        
    Returns:
        bool: True if successful, False otherwise
    """
    s3 = S3Manager()
    output_key = "transform/folder_2_filter_data/combined_meal_data_filtered.xlsx"
    temp_output = "temp_output.xlsx"
    
    try:
        # Save to temporary file
        df.to_excel(temp_output, index=False)
        
        # Upload to S3
        return s3.upload_file(temp_output, output_key)
    except Exception as e:
        print(f"Error saving data to S3: {str(e)}")
        return False
    finally:
        if os.path.exists(temp_output):
            os.remove(temp_output)

def clean_meal_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean meal data by removing null values and outliers.
    Logs deleted rows and duplicates.

    Args:
        df: Input DataFrame to clean

    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    print("\nColumns in DataFrame:", df.columns.tolist())
    
    # Log for deleted rows
    deleted_rows_log = []

    # 1. Remove records with null values in critical columns
    critical_columns = [
        "aliment_id",
        "quantity",
        "Valeur calorique",  # Original column name
        "Lipides",           # Original column name
        "Glucides",          # Original column name
        "Protein"            # Original column name
    ]
    
    # Verify all critical columns exist
    missing_columns = [col for col in critical_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing critical columns: {missing_columns}")
        
    df_cleaned = df.dropna(subset=critical_columns)
    print(f"\nRows removed due to null values: {len(df) - len(df_cleaned)}")
    
    # Log deleted rows
    deleted_rows_log.append(df[~df.index.isin(df_cleaned.index)])

    # 2. Remove records with invalid units (like negative values)
    df_cleaned = df_cleaned[df_cleaned["Valeur calorique"] >= 0]
    print(f"Rows removed due to negative calories: {len(df) - len(df_cleaned)}")
    
    # Log deleted rows for this step
    deleted_rows_log.append(df[~df.index.isin(df_cleaned.index)])

    # 3. Check extreme values for calories per quantity and high quantities
    df_cleaned["quantity_status"] = df_cleaned.apply(
        lambda row: (
            "High"
            if row["Valeur calorique"] > 5000
               or row["quantity"] > 100
            else "Normal"
        ),
        axis=1,
    )

    # Remove rows marked as "High"
    high_values_mask = df_cleaned["quantity_status"] == "High"
    deleted_rows_log.append(df_cleaned[high_values_mask])
    df_cleaned = df_cleaned[~high_values_mask]
    print(f"Rows removed due to high values: {high_values_mask.sum()}")

    # 4. Analyze null or outlier values by user
    user_nulls = df.isnull().groupby(df["user_id"]).sum()
    user_outliers = df_cleaned.groupby("user_id")["quantity_status"].apply(
        lambda x: (x != "Normal").sum()
    )

    # 5. Log deleted rows
    deleted_rows = pd.concat(deleted_rows_log).drop_duplicates()
    deleted_rows.to_csv("deleted_rows_log.csv", index=False)

    # 6. Check for duplicates after cleaning
    df_cleaned = df_cleaned.drop_duplicates()
    print(f"Duplicates removed: {len(df) - len(df_cleaned)}")

    # Print analysis results
    print("\nNull values by user:")
    print(user_nulls)
    print("\nOutliers by user:")
    print(user_outliers)
    print(f"\nTotal rows removed: {len(df) - len(df_cleaned)}")

    return df_cleaned

def main():
    """
    Main function to filter data
    """
    try:
        print("Loading data from S3...")
        input_df = load_from_s3()
        
        if input_df is not None:
            print("Applying cleaning filters...")
            filtered_df = clean_meal_data(input_df)
            print("Saving filtered data to S3...")
            if save_to_s3(filtered_df):
                print("Successfully saved filtered data to S3")
                print(f"Final number of records: {len(filtered_df)}")
            else:
                print("Failed to save filtered data to S3")
            return filtered_df
        else:
            print("Failed to load data from S3")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()