import pandas as pd
import numpy as np


def load_and_prepare_data(pandas_file, duckdb_file):
    # Charger les données depuis les fichiers Excel
    df_pandas = pd.read_excel(pandas_file)
    df_duckdb = pd.read_excel(duckdb_file)

    # Mapper les noms des jours de la semaine (en Pandas) à leurs valeurs numériques (en DuckDB)
    day_of_week_mapping = {
        'Monday': 1, 'Tuesday': 2, 'Wednesday': 3, 'Thursday': 4,
        'Friday': 5, 'Saturday': 6, 'Sunday': 7
    }

    # Convertir les jours de la semaine en valeurs numériques dans le DataFrame pandas
    df_pandas['day_of_week'] = df_pandas['day_of_week'].map(day_of_week_mapping)

    # Remplacer les valeurs booléennes dans pandas par des 0 (False) et 1 (True)
    df_pandas.replace({True: 1, False: 0}, inplace=True)

    # Sélectionner les colonnes communes
    common_columns = list(set(df_pandas.columns) & set(df_duckdb.columns))
    df_pandas = df_pandas[common_columns]
    df_duckdb = df_duckdb[common_columns]

    return df_pandas, df_duckdb


def compare_dataframes(df_pandas, df_duckdb):
    # Fusionner les deux DataFrames sur les colonnes 'date' et 'user_id'
    merged_df = pd.merge(df_pandas, df_duckdb, on=['date', 'user_id'], suffixes=('_pandas', '_duckdb'))

    # Identifier les colonnes à comparer
    comparison_columns = [col for col in df_pandas.columns if col not in ['date', 'user_id']]

    # Comparer les colonnes et ajouter un suffixe _comparison pour chaque comparaison
    for col in comparison_columns:
        merged_df[f'{col}_comparison'] = merged_df[f'{col}_pandas'] == merged_df[f'{col}_duckdb']

    return merged_df


def save_comparison_to_excel(merged_df, output_file):
    # Sélectionner uniquement les colonnes importantes (date, user_id, et colonnes de comparaison)
    comparison_columns = [col for col in merged_df.columns if '_comparison' in col]
    output_df = merged_df[['date', 'user_id'] + comparison_columns]

    # Sauvegarder les résultats dans un fichier Excel
    output_df.to_excel(output_file, index=False)


if __name__ == "__main__":
    # Charger et préparer les données
    df_pandas, df_duckdb = load_and_prepare_data(
        'data/pandas_window_function_results.xlsx',
        'data/duckdb_window_function_results.xlsx'
    )

    # Comparer les DataFrames
    merged_df = compare_dataframes(df_pandas, df_duckdb)

    # Enregistrer les résultats de la comparaison dans un fichier Excel
    save_comparison_to_excel(merged_df, 'data/comparison_results.xlsx')

    print("Comparaison terminée. Les résultats ont été sauvegardés dans 'data/comparison_results.xlsx'.")
