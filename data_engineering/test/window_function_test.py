import os
import unittest
import pandas as pd
from data_engineering.Transform_data.windows_function import pandas_window_function, duckdb_window_function

# Chemin de base pour les fichiers de test
FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data'

class TestDataFunctions(unittest.TestCase):

    def setUp(self):
        # Exécuter les deux fonctions pour générer les fichiers de données
        self.df_pandas = pandas_window_function()
        self.df_duckdb = duckdb_window_function()

        # Charger les fichiers Excel générés pour les comparer
        pandas_file = os.path.join(FILE_PATH, 'pandas_window_function_results.xlsx')
        duckdb_file = os.path.join(FILE_PATH, 'duckdb_window_function_results.xlsx')

        # Charger les données à partir des fichiers Excel
        self.df_pandas = pd.read_excel(pandas_file)
        self.df_duckdb = pd.read_excel(duckdb_file)

    def test_shape_equality(self):
        # Vérifier si les DataFrames générés ont le même nombre de lignes et de colonnes
        self.assertEqual(self.df_pandas.shape, self.df_duckdb.shape,
                         "Les DataFrames n'ont pas la même taille.")

    def test_column_names(self):
        # Vérifier si les noms des colonnes sont identiques dans les deux DataFrames
        self.assertListEqual(list(self.df_pandas.columns), list(self.df_duckdb.columns),
                             "Les noms des colonnes sont différents.")

    def test_data_types(self):
        # Vérifier si les types de données des colonnes sont les mêmes entre Pandas et DuckDB
        pandas_dtypes = self.df_pandas.dtypes
        duckdb_dtypes = self.df_duckdb.dtypes
        for col in self.df_pandas.columns:
            self.assertEqual(pandas_dtypes[col], duckdb_dtypes[col],
                             f"Le type de données de la colonne '{col}' est différent entre Pandas et DuckDB.")

    def test_data_consistency(self):
        # Vérifier si les données des deux DataFrames sont égales
        for col in self.df_pandas.columns:
            pd.testing.assert_series_equal(self.df_pandas[col], self.df_duckdb[col],
                                           obj=f"Les données de la colonne '{col}' ne sont pas égales.")

if __name__ == '__main__':
    unittest.main()
