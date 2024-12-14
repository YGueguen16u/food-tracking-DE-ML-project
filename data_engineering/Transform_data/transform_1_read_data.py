"""
This module reads the combined meal data from S3 and performs initial data validation.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

def read_combined_data_from_s3():
    """
    Read the combined meal data from S3.
    
    Returns:
        pd.DataFrame: Combined meal data with nutritional values
    """
    s3 = S3Manager()
    s3_key = "transform/folder_1_combine/combined_meal_data.xlsx"
    
    # Download file temporarily
    temp_file = "temp_combined_data.xlsx"
    try:
        if s3.download_file(s3_key, temp_file):
            # Read the Excel file
            df = pd.read_excel(temp_file)
            
            # Validate required columns
            required_columns = [
                "meal_record_id", "date", "heure", "user_id",
                "aliment_id", "quantity", "total_calories",
                "total_lipids", "total_carbs", "total_protein"
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Convert date and time columns
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["heure"] = pd.to_datetime(df["heure"], format="%H:%M:%S", errors="coerce").dt.time
            
            # Sort by date and time
            df = df.sort_values(by=["date", "heure"])
            
            # Basic data validation
            if df["total_calories"].isnull().any():
                print("Warning: Found null values in total_calories")
            
            if df["quantity"].isnull().any():
                print("Warning: Found null values in quantity")
            
            return df
            
        else:
            raise FileNotFoundError(f"Could not download file from S3: {s3_key}")
            
    except Exception as e:
        print(f"Error reading combined data: {str(e)}")
        raise
        
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file):
            os.remove(temp_file)

def main():
    """
    Main function to read and validate the combined meal data
    """
    try:
        print("Reading combined meal data from S3...")
        df = read_combined_data_from_s3()
        print(f"Successfully loaded {len(df)} records")
        print("\nDataset Overview:")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"Number of unique users: {df['user_id'].nunique()}")
        print(f"Number of unique meals: {df['meal_id'].nunique()}")
        print("\nNutritional Statistics:")
        print(df[["total_calories", "total_lipids", "total_carbs", "total_protein"]].describe())
        
        return df
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()