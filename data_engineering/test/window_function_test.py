import os
import unittest
import pandas as pd

# La fonction à tester
def load_and_prepare_data(pandas_file, duckdb_file):
    # Charger les données depuis les fichiers Excel
    df_pandas = pd.read_excel(pandas_file)
    df_duckdb = pd.read_excel(duckdb_file)

    # Convertir les colonnes 'date' en datetime dans Pandas
    df_pandas['date'] = pd.to_datetime(df_pandas['date'])

    # Utiliser dt.weekday pour obtenir les jours de la semaine en valeurs numériques (Lundi = 1, Dimanche = 7)
    df_pandas['day_of_week'] = df_pandas['date'].dt.weekday + 1  # Lundi = 1, ..., Dimanche = 7

    # Remplacer les valeurs booléennes dans pandas par des 0 (False) et 1 (True)
    df_pandas.replace({True: 1, False: 0}, inplace=True)

    # Sélectionner les colonnes communes
    common_columns = list(set(df_pandas.columns) & set(df_duckdb.columns))
    df_pandas = df_pandas[common_columns]
    df_duckdb = df_duckdb[common_columns]

    return df_pandas, df_duckdb


# Fonction pour comparer les DataFrames
def compare_dataframes(df_pandas, df_duckdb):
    merged_df = pd.merge(df_pandas, df_duckdb, on=['date', 'user_id'], suffixes=('_pandas', '_duckdb'))

    comparison_columns = [col for col in df_pandas.columns if col not in ['date', 'user_id']]

    for col in comparison_columns:
        if pd.api.types.is_numeric_dtype(merged_df[f'{col}_pandas']):
            # Afficher les différences dans les colonnes d'origine avant de comparer les différences
            if col == 'total_lipids':
                non_matching = merged_df[merged_df[f'{col}_pandas'] != merged_df[f'{col}_duckdb']]
                if not non_matching.empty:
                    print(f"\nDifférences dans la colonne '{col}' (avant calcul des diff) :")
                    print(non_matching[['date', 'user_id', f'{col}_pandas', f'{col}_duckdb']])

            # Arrondir à 3 décimales avant de comparer
            merged_df[f'{col}_pandas'] = merged_df[f'{col}_pandas'].round(3)
            merged_df[f'{col}_duckdb'] = merged_df[f'{col}_duckdb'].round(3)
        merged_df[f'{col}_comparison'] = merged_df[f'{col}_pandas'] == merged_df[f'{col}_duckdb']

    return merged_df



# Fonction pour sauvegarder les résultats de la comparaison
def save_comparison_to_excel(merged_df, output_file):
    comparison_columns = [col for col in merged_df.columns if '_comparison' in col]
    output_df = merged_df[['date', 'user_id'] + comparison_columns]

    # Enregistrer également les valeurs de total_carbs pour voir les écarts exacts
    output_df['total_carbs_pandas'] = merged_df['total_carbs_pandas']
    output_df['total_carbs_duckdb'] = merged_df['total_carbs_duckdb']

    output_df.to_excel(output_file, index=False)


# Test unitaire avec la fonction directement incluse
class TestDataComparison(unittest.TestCase):

    # Chemin de base pour les fichiers de test
    FILE_PATH = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\data_engineering\Transform_data\data'

    def setUp(self):
        # Chemins vers les fichiers générés par les fonctions pandas et duckdb
        self.pandas_file = os.path.join(self.FILE_PATH, 'pandas_window_function_results.xlsx')
        self.duckdb_file = os.path.join(self.FILE_PATH, 'duckdb_window_function_results.xlsx')

        # Charger et préparer les données pour comparaison
        self.df_pandas, self.df_duckdb = load_and_prepare_data(self.pandas_file, self.duckdb_file)

    def test_load_and_prepare_data(self):
        # Vérifier que les DataFrames ne sont pas vides après le chargement
        self.assertFalse(self.df_pandas.empty, "Le DataFrame Pandas est vide après le chargement.")
        self.assertFalse(self.df_duckdb.empty, "Le DataFrame DuckDB est vide après le chargement.")

        # Vérifier que les deux DataFrames ont les mêmes colonnes après la sélection des colonnes communes
        self.assertEqual(set(self.df_pandas.columns), set(self.df_duckdb.columns), "Les colonnes communes ne correspondent pas.")

    def test_compare_dataframes(self):
        # Comparer les deux DataFrames
        merged_df = compare_dataframes(self.df_pandas, self.df_duckdb)

        # Vérifier que la fusion a eu lieu correctement sur 'date' et 'user_id'
        self.assertIn('date', merged_df.columns, "La colonne 'date' manque dans le DataFrame fusionné.")
        self.assertIn('user_id', merged_df.columns, "La colonne 'user_id' manque dans le DataFrame fusionné.")

        # Vérifier que les colonnes de comparaison ont été créées
        comparison_columns = [col for col in self.df_pandas.columns if col not in ['date', 'user_id']]
        for col in comparison_columns:
            self.assertIn(f'{col}_comparison', merged_df.columns, f"La colonne de comparaison '{col}_comparison' n'a pas été créée.")

        # Afficher les différences dans la colonne qui échoue
        for col in comparison_columns:
            non_matching = merged_df[merged_df[f'{col}_comparison'] == False]
            if not non_matching.empty:
                print(f"\nDifférences dans la colonne '{col}':")
                print(non_matching[['date', 'user_id', f'{col}_pandas', f'{col}_duckdb']])
            comparison_result = merged_df[f'{col}_comparison'].all()
            self.assertTrue(comparison_result, f"Les données de la colonne '{col}' ne correspondent pas pour certaines lignes.")

    def test_save_comparison_to_excel(self):
        # Comparer les DataFrames
        merged_df = compare_dataframes(self.df_pandas, self.df_duckdb)

        # Sauvegarder les résultats de la comparaison dans un fichier Excel
        output_file = os.path.join(self.FILE_PATH, 'comparison_results.xlsx')
        save_comparison_to_excel(merged_df, output_file)

        # Vérifier que le fichier a été créé
        self.assertTrue(os.path.exists(output_file), "Le fichier de résultats de comparaison n'a pas été généré.")

        # Charger le fichier généré pour vérifier son contenu
        output_df = pd.read_excel(output_file)
        self.assertFalse(output_df.empty, "Le fichier Excel de comparaison est vide.")

if __name__ == '__main__':
    unittest.main()
