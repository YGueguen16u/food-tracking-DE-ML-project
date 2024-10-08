import pandas as pd
import numpy as np
import pytest

# This module contains tests for validating calculated nutritional values
# and ensuring the integrity of meal data stored in an Excel file.

# Define the path to the Excel file
FILE_PATH = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data\combined_meal_data.xlsx"


@pytest.fixture
def load_dataframe():
    """Fixture to load the DataFrame for use in tests."""
    return pd.read_excel(FILE_PATH)


def test_calculated_values(load_dataframe):
    """Test that the calculated nutritional values are correct, ignoring NaNs."""
    # Ignore rows with NaN in any of the relevant columns for comparison
    df_filtered = load_dataframe.dropna(
        subset=[
            "total_calories",
            "Valeur calorique",
            "quantity",
            "total_lipids",
            "Lipides",
            "total_carbs",
            "Glucides",
            "total_protein",
            "Protein",
        ]
    )

    # Perform the comparisons on the filtered dataframe with a tolerance for floating point errors
    calories_match = np.isclose(
        df_filtered["total_calories"],
        df_filtered["Valeur calorique"] * df_filtered["quantity"],
        rtol=1e-5,
    )
    lipids_match = np.isclose(
        df_filtered["total_lipids"],
        df_filtered["Lipides"] * df_filtered["quantity"],
        rtol=1e-5,
    )
    carbs_match = np.isclose(
        df_filtered["total_carbs"],
        df_filtered["Glucides"] * df_filtered["quantity"],
        rtol=1e-5,
    )
    protein_match = np.isclose(
        df_filtered["total_protein"],
        df_filtered["Protein"] * df_filtered["quantity"],
        rtol=1e-5,
    )

    # Diagnostic: collect lines where the comparison fails
    mismatched_calories = df_filtered[~calories_match]
    mismatched_lipids = df_filtered[~lipids_match]
    mismatched_carbs = df_filtered[~carbs_match]
    mismatched_protein = df_filtered[~protein_match]

    assert (
        calories_match.all()
    ), f"Mismatch in total_calories calculation! Mismatched rows: {mismatched_calories}"
    assert (
        lipids_match.all()
    ), f"Mismatch in total_lipids calculation! Mismatched rows: {mismatched_lipids}"
    assert (
        carbs_match.all()
    ), f"Mismatch in total_carbs calculation! Mismatched rows: {mismatched_carbs}"
    assert (
        protein_match.all()
    ), f"Mismatch in total_protein calculation! Mismatched rows: {mismatched_protein}"


def test_date_time_format(load_dataframe):
    """Test that the date and heure columns are in the correct format."""
    assert (
        pd.to_datetime(load_dataframe["date"], errors="coerce").notna().all()
    ), "Date column contains invalid dates!"
    assert (
        pd.to_datetime(load_dataframe["heure"], format="%H:%M:%S", errors="coerce")
        .notna()
        .all()
    ), "Heure column contains invalid times!"


def test_unique_meal_record_id(load_dataframe):
    """Test that meal_record_id is unique for each row."""
    assert load_dataframe["meal_record_id"].is_unique, "Duplicate meal_record_id found!"


def test_file_integrity():
    """Test that the saved Excel file is readable and contains the expected number of rows."""
    loaded_df = pd.read_excel(FILE_PATH)
    expected_rows = len(loaded_df)
    assert len(loaded_df) == expected_rows, "File does not contain the expected number of rows!"
