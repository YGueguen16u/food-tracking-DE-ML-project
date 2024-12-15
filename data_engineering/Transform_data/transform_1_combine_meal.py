"""
This module processes and combines multiple CSV files containing meal data from S3,
merges them with a reference aliment table, and calculates total nutritional
values before saving the final dataset as an Excel file back to S3.
"""

import os
import sys
import io
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

def download_and_process_s3_files():
    """Download and process CSV files from S3"""
    s3 = S3Manager()
    dataframes = []
    
    # List all files in the raw_data_ingestion directory
    files = s3.list_files("raw_data_ingestion/")
    
    for file_path in files:
        if file_path.endswith('.csv'):
            try:
                # Download file from S3 to memory
                local_path = f"temp_{os.path.basename(file_path)}"
                if s3.download_file(file_path, local_path):
                    df = pd.read_csv(local_path)
                    dataframes.append(df)
                    os.remove(local_path)  # Clean up temporary file
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
    
    return dataframes


def main():
    """
    Main function to combine meal data
    """
    try:
        print("Combining meal data...")
        # Initialize S3 manager
        s3 = S3Manager()

        # Load all CSV files from S3 into a single DataFrame
        print("Downloading and processing files from S3...")
        dataframes = download_and_process_s3_files()

        # Concatenate all DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)

        # Handle potential missing or misformatted columns
        if "date" in combined_df.columns and "heure" in combined_df.columns:
            # Sort the DataFrame by date and heure
            combined_df["date"] = pd.to_datetime(combined_df["date"], errors="coerce")
            combined_df["heure"] = pd.to_datetime(
                combined_df["heure"], format="%H:%M:%S", errors="coerce"
            ).dt.time
            combined_df = combined_df.sort_values(by=["date", "heure"])
        else:
            raise ValueError(
                "The required columns 'date' or 'heure' are missing from the combined DataFrame."
            )

        # Generate a unique meal_record_id for each row starting from 1
        combined_df["meal_record_id"] = range(1, len(combined_df) + 1)

        # Move meal_record_id to the first column
        cols = ["meal_record_id"] + [
            col for col in combined_df.columns if col != "meal_record_id"
        ]
        combined_df = combined_df[cols]

        # Load aliment reference table from S3
        temp_food_file = "temp_food_data.xlsx"
        try:
            if s3.download_file("reference_data/food/food_processed.xlsx", temp_food_file):
                aliment_table = pd.read_excel(
                    temp_food_file,
                    usecols=[
                        "id",
                        "Aliment",
                        "Valeur calorique",
                        "Lipides",
                        "Glucides",
                        "Protein",
                    ],
                )
            else:
                raise FileNotFoundError("Could not download food data from S3")
        finally:
            if os.path.exists(temp_food_file):
                os.remove(temp_food_file)

        # Rename 'id' in the aliment table to 'aliment_id' to match the combined_df
        aliment_table = aliment_table.rename(columns={"id": "aliment_id"})

        # Ensure 'aliment_id' exists in both DataFrames before merging
        if "aliment_id" in combined_df.columns and "aliment_id" in aliment_table.columns:
            merged_df = pd.merge(combined_df, aliment_table, on="aliment_id", how="left")
        else:
            raise ValueError(
                "The column 'aliment_id' is missing in either the combined DataFrame or aliment table."
            )

        # Calculate total nutritional values based on quantity
        merged_df["total_calories"] = merged_df["Valeur calorique"] * merged_df["quantity"]
        merged_df["total_lipids"] = merged_df["Lipides"] * merged_df["quantity"]
        merged_df["total_carbs"] = merged_df["Glucides"] * merged_df["quantity"]
        merged_df["total_protein"] = merged_df["Protein"] * merged_df["quantity"]

        # Reorder columns to have meal_record_id at the beginning
        final_df = merged_df[
            ["meal_record_id"] + [col for col in merged_df.columns if col != "meal_record_id"]
        ]

        # Save to temporary Excel file
        temp_excel_path = "temp_combined_meal_data.xlsx"
        final_df.to_excel(temp_excel_path, index=False)

        # Upload to S3
        s3_key = "transform/folder_1_combine/combined_meal_data.xlsx"
        print(f"Uploading final Excel file to S3: {s3_key}")
        if s3.upload_file(temp_excel_path, s3_key):
            print("File uploaded successfully to S3")
        else:
            print("Error uploading file to S3")

        # Clean up temporary file
        os.remove(temp_excel_path)
        
        print("Data combined successfully!")
        return final_df
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()