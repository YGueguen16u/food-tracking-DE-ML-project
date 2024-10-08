import os
import pandas as pd

# Path to the directory containing the CSV files
DIRECTORY_PATH = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Extract_data\data\raw"

# Initialize an empty list to store DataFrames
dataframes = []

# Load all CSV files into a single DataFrame
for filename in os.listdir(DIRECTORY_PATH):
    if filename.endswith(".csv"):
        file_path = os.path.join(DIRECTORY_PATH, filename)
        try:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        except Exception as e:
            print(f"Error loading {filename}: {e}")

# Concatenate all DataFrames
combined_df = pd.concat(dataframes, ignore_index=True)

# Handle potential missing or misformatted columns
if 'date' in combined_df.columns and 'heure' in combined_df.columns:
    # Sort the DataFrame by date and heure
    combined_df["date"] = pd.to_datetime(combined_df["date"], errors="coerce")
    combined_df["heure"] = pd.to_datetime(combined_df["heure"], format="%H:%M:%S", errors="coerce").dt.time
    combined_df = combined_df.sort_values(by=["date", "heure"])
else:
    raise ValueError("The required columns 'date' or 'heure' are missing from the combined DataFrame.")

# Generate a unique meal_record_id for each row starting from 1
combined_df["meal_record_id"] = range(1, len(combined_df) + 1)

# Move meal_record_id to the first column
cols = ["meal_record_id"] + [col for col in combined_df.columns if col != "meal_record_id"]
combined_df = combined_df[cols]

# Load aliment reference table
aliment_table = pd.read_excel(
    r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\raw_food_data\food_processed.xlsx",
    usecols=["id", "Aliment", "Valeur calorique", "Lipides", "Glucides", "Protein"]
)

# Rename 'id' in the aliment table to 'aliment_id' to match the combined_df
aliment_table = aliment_table.rename(columns={"id": "aliment_id"})

# Ensure 'aliment_id' exists in both DataFrames before merging
if 'aliment_id' in combined_df.columns and 'aliment_id' in aliment_table.columns:
    merged_df = pd.merge(combined_df, aliment_table, on="aliment_id", how="left")
else:
    raise ValueError("The column 'aliment_id' is missing in either the combined DataFrame or aliment table.")

# Calculate total nutritional values based on quantity
merged_df["total_calories"] = merged_df["Valeur calorique"] * merged_df["quantity"]
merged_df["total_lipids"] = merged_df["Lipides"] * merged_df["quantity"]
merged_df["total_carbs"] = merged_df["Glucides"] * merged_df["quantity"]
merged_df["total_protein"] = merged_df["Protein"] * merged_df["quantity"]

# Reorder columns to have meal_record_id at the beginning
final_df = merged_df[["meal_record_id"] + [col for col in merged_df.columns if col != "meal_record_id"]]

# Vérifier si le dossier 'data' existe, sinon le créer
OUTPUT_DIRECTORY = "data"
if not os.path.exists(OUTPUT_DIRECTORY):
    os.makedirs(OUTPUT_DIRECTORY)

# Sauvegarder le DataFrame final dans un fichier Excel
final_df.to_excel(os.path.join(OUTPUT_DIRECTORY, "combined_meal_data.xlsx"), index=False)

print("File saved successfully.")
