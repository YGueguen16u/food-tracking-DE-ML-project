"""
Ce module contient des tests unitaires pour la fonction clean_meal_data
qui nettoie et filtre les données de repas.
"""

import os
import unittest

import pandas as pd

from data_engineering.Transform_data.transform_2_filter_data import clean_meal_data


class TestCleanMealData(unittest.TestCase):
    """
    Classe de tests pour la fonction clean_meal_data.
    """

    def setUp(self):
        """
        Préparation des données de test et des fichiers temporaires avant chaque test.
        """
        # Set up test data
        data = {
            "meal_record_id": [1, 2, 3, 4],
            "user_id": [19, 19, 19, 19],
            "meal_id": [1, 1, 1, 1],
            "date": ["2024-06-25", "2024-06-25", "2024-06-25", "2024-06-25"],
            "heure": ["07:02:00", "07:02:00", "07:02:00", "07:02:00"],
            "aliment_id": [None, 328, 330, None],
            "quantity": [1, 1, 1, 1],
            "Valeur calorique": [93, 93, 55, None],
            "Lipides": [6.4, 6.4, 4.5, None],
            "Glucides": [0.7, 0.7, 0.6, None],
            "Protein": [7.5, 7.5, 2.7, None],
            "total_calories": [93, 93, 55, None],
            "total_lipids": [6.4, 6.4, 4.5, None],
            "total_carbs": [0.7, 0.7, 0.6, None],
            "total_protein": [7.5, 7.5, 2.7, None],
        }
        self.test_df = pd.DataFrame(data)

        # Save to a temporary file for testing
        self.test_input_file = "test_input.xlsx"
        self.test_output_file = "test_output.xlsx"
        self.test_df.to_excel(self.test_input_file, index=False)

    def tearDown(self):
        """
        Nettoyage après chaque test, suppression des fichiers temporaires.
        """

        # Remove the temporary files
        if os.path.exists(self.test_input_file):
            os.remove(self.test_input_file)
        if os.path.exists(self.test_output_file):
            os.remove(self.test_output_file)

    def test_clean_meal_data(self):
        """
        Teste la fonction clean_meal_data pour s'assurer que les données sont
        correctement nettoyées, les fichiers de sortie sont générés
        et les colonnes critiques ne contiennent pas de valeurs nulles.
        """

        # Run the cleaning function with updated paths
        clean_meal_data(
            self.test_input_file,
            self.test_output_file,
            threshold_calories_high=1000,
            threshold_quantity_high=10,
        )

        # Check if output file is created
        self.assertTrue(os.path.exists(self.test_output_file))

        # Load the output and check the correctness
        output_df = pd.read_excel(self.test_output_file)

        # Assert the number of rows is as expected after cleaning
        self.assertEqual(
            len(output_df), 2
        )  # Only rows with aliment_id not None should remain

        # Assert no null values in critical columns
        self.assertTrue(
            output_df[
                [
                    "aliment_id",
                    "quantity",
                    "Valeur calorique",
                    "Lipides",
                    "Glucides",
                    "Protein",
                ]
            ]
            .notnull()
            .all()
            .all()
        )

        # Assert extreme_quantity column exists
        self.assertIn("extreme_quantity", output_df.columns)


if __name__ == "__main__":
    unittest.main()
