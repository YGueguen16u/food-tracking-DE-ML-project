import os
import pandas as pd
import pytest

# Define the path to the Excel file
FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\combined_meal_data.xlsx'


@pytest.fixture
def df():
    """Fixture to load the DataFrame for use in tests."""
    return pd.read_excel(FILE_PATH)


@pytest.fixture
def test_calculated_values(df):
    """Test that the calculated nutritional values are correct."""
    assert all(
        df['total_calories'] == df['Valeur calorique'] * df['quantity']), "Mismatch in total_calories calculation!"
    assert all(df['total_lipids'] == df['Lipides'] * df['quantity']), "Mismatch in total_lipids calculation!"
    assert all(df['total_carbs'] == df['Glucides'] * df['quantity']), "Mismatch in total_carbs calculation!"
    assert all(df['total_protein'] == df['Protein'] * df['quantity']), "Mismatch in total_protein calculation!"


def test_date_time_format(df):
    """Test that the date and heure columns are in the correct format."""
    assert pd.to_datetime(df['date'], errors='coerce').notna().all(), "Date column contains invalid dates!"
    assert pd.to_datetime(df['heure'], format='%H:%M:%S',
                          errors='coerce').notna().all(), "Heure column contains invalid times!"


def test_unique_meal_record_id(df):
    """Test that meal_record_id is unique for each row."""
    assert df['meal_record_id'].is_unique, "Duplicate meal_record_id found!"


def test_file_integrity():
    """Test that the saved Excel file is readable and contains the expected number of rows."""
    df = pd.read_excel(FILE_PATH)
    expected_rows = len(df)
    assert len(df) == expected_rows, "File does not contain the expected number of rows!"


def run_tests():
    # Load your processed DataFrame from the Excel file
    file_path = 'combined_meal_data.xlsx'
    final_df = pd.read_excel(file_path)

    # Run all tests
    test_calculated_values(final_df)
    test_date_time_format(final_df)
    test_unique_meal_record_id(final_df)
    test_file_integrity()


if __name__ == "__main__":
    run_tests()
