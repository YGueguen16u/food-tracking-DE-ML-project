import pandas as pd
import numpy as np
import duckdb

def pandas_percentage_change():
    # Charger les données à partir du fichier Excel
    df = pd.read_excel('data/pandas_window_function_results.xlsx')

    # Calcul du pourcentage de changement pour chaque nutriment
    df['pct_change_calories'] = ((df['total_calories'] - df['avg_last_4_calories']) / df['avg_last_4_calories']) * 100
    df['pct_change_lipids'] = ((df['total_lipids'] - df['avg_last_4_lipids']) / df['avg_last_4_lipids']) * 100
    df['pct_change_carbs'] = ((df['total_carbs'] - df['avg_last_4_carbs']) / df['avg_last_4_carbs']) * 100
    df['pct_change_protein'] = ((df['total_protein'] - df['avg_last_4_protein']) / df['avg_last_4_protein']) * 100

    # Ajouter une alerte pour les changements qui dépassent +/- 20%
    df['calories_alert'] = np.abs(df['pct_change_calories']) > 20
    df['lipids_alert'] = np.abs(df['pct_change_lipids']) > 20
    df['carbs_alert'] = np.abs(df['pct_change_carbs']) > 20
    df['protein_alert'] = np.abs(df['pct_change_protein']) > 20

    # Sauvegarder les résultats dans un fichier Excel
    df.to_excel('data/pandas_percentage_change_results.xlsx', index=False)

    return df


def duckdb_percentage_change():
    # Charger les données à partir du fichier Excel
    df = pd.read_excel('data/duckdb_window_function_results.xlsx')

    # Créer une connexion DuckDB en mémoire
    conn = duckdb.connect(database=':memory:')

    # Charger le DataFrame Pandas dans DuckDB
    conn.register('df', df)

    # Requête SQL DuckDB pour calculer le pourcentage de changement
    query = """
    WITH PercentageChanges AS (
        SELECT *,
            ((total_calories - avg_last_4_calories) / avg_last_4_calories) * 100 AS pct_change_calories,
            ((total_lipids - avg_last_4_lipids) / avg_last_4_lipids) * 100 AS pct_change_lipids,
            ((total_carbs - avg_last_4_carbs) / avg_last_4_carbs) * 100 AS pct_change_carbs,
            ((total_protein - avg_last_4_protein) / avg_last_4_protein) * 100 AS pct_change_protein
        FROM df
    ),
    Alerts AS (
        SELECT *,
            CASE WHEN ABS(pct_change_calories) > 20 THEN 1 ELSE 0 END AS calories_alert,
            CASE WHEN ABS(pct_change_lipids) > 20 THEN 1 ELSE 0 END AS lipids_alert,
            CASE WHEN ABS(pct_change_carbs) > 20 THEN 1 ELSE 0 END AS carbs_alert,
            CASE WHEN ABS(pct_change_protein) > 20 THEN 1 ELSE 0 END AS protein_alert
        FROM PercentageChanges
    )
    SELECT * FROM Alerts
    """

    # Exécuter la requête et récupérer les résultats
    result_df = conn.execute(query).df()

    # Sauvegarder les résultats dans un fichier Excel
    result_df.to_excel('data/duckdb_percentage_change_results.xlsx', index=False)

    return result_df
