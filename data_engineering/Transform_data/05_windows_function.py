import pandas as pd
import numpy as np
import duckdb


def pandas_window_function():
    # Charger les données à partir du fichier Excel
    df = pd.read_excel('data/pandas_aggregation_results.xlsx', sheet_name='User Daily Aggregation')

    # Convertir la colonne 'date' en format datetime
    df['date'] = pd.to_datetime(df['date'])

    # Extraire le jour de la semaine pour chaque date
    df['day_of_week'] = df['date'].dt.day_name()

    # Trier les données par user_id, day_of_week, puis par date
    df = df.sort_values(by=['user_id', 'day_of_week', 'date'])

    # Fenêtrage pour calculer la moyenne sur les 4 derniers jours similaires
    df['avg_last_4_calories'] = df.groupby(['user_id', 'day_of_week'])['total_calories'].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df['avg_last_4_lipids'] = df.groupby(['user_id', 'day_of_week'])['total_lipids'].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df['avg_last_4_carbs'] = df.groupby(['user_id', 'day_of_week'])['total_carbs'].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df['avg_last_4_protein'] = df.groupby(['user_id', 'day_of_week'])['total_protein'].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

    # Ajouter une colonne qui compare les valeurs actuelles de nutriments avec la moyenne des 4 derniers
    df['calories_diff'] = df['total_calories'] - df['avg_last_4_calories']
    df['lipids_diff'] = df['total_lipids'] - df['avg_last_4_lipids']
    df['carbs_diff'] = df['total_carbs'] - df['avg_last_4_carbs']
    df['protein_diff'] = df['total_protein'] - df['avg_last_4_protein']

    # Identifier les jours où la consommation dévie fortement de la moyenne (par exemple, écart-type)
    df['calories_std_dev'] = df.groupby(['user_id', 'day_of_week'])['total_calories'].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    df['lipids_std_dev'] = df.groupby(['user_id', 'day_of_week'])['total_lipids'].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    df['carbs_std_dev'] = df.groupby(['user_id', 'day_of_week'])['total_carbs'].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    df['protein_std_dev'] = df.groupby(['user_id', 'day_of_week'])['total_protein'].transform(lambda x: x.rolling(window=4, min_periods=1).std())

    # Marquer les jours où la déviation est supérieure à 2 écarts-types
    df['calories_outlier'] = np.abs(df['calories_diff']) > 2 * df['calories_std_dev']
    df['lipids_outlier'] = np.abs(df['lipids_diff']) > 2 * df['lipids_std_dev']
    df['carbs_outlier'] = np.abs(df['carbs_diff']) > 2 * df['carbs_std_dev']
    df['protein_outlier'] = np.abs(df['protein_diff']) > 2 * df['protein_std_dev']

    # Sauvegarder les résultats dans un fichier Excel
    df.to_excel('data/pandas_window_function_results.xlsx', index=False)

    return df

def duckdb_window_function():
    # Charger les données à partir du fichier Excel
    df = pd.read_excel('data/duckdb_aggregation_results.xlsx', sheet_name='User Daily Aggregation')

    # Convertir la colonne 'date' en format datetime
    df['date'] = pd.to_datetime(df['date'])

    # Créer une connexion DuckDB en mémoire
    conn = duckdb.connect(database=':memory:')

    # Charger le DataFrame Pandas dans DuckDB
    conn.register('df', df)

    # Ajouter la colonne 'day_of_week' dans DuckDB en utilisant une requête SQL
    conn.execute("""
    CREATE TABLE df_with_day_of_week AS
    SELECT *,
           STRFTIME('%w', date) AS day_of_week
    FROM df
    """)

    # Exécuter la requête pour calculer les fenêtres
    query = """
    WITH RankedData AS (
        SELECT 
            date,
            user_id,
            total_calories,
            total_lipids,
            total_carbs,
            total_protein,
            STRFTIME('%w', date) AS day_of_week,
            ROW_NUMBER() OVER(PARTITION BY user_id, STRFTIME('%w', date) ORDER BY date DESC) AS rn
        FROM df_with_day_of_week
    ),
    WindowedAverages AS (
        SELECT 
            *,
            AVG(total_calories) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_last_4_calories,
            AVG(total_lipids) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_last_4_lipids,
            AVG(total_carbs) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_last_4_carbs,
            AVG(total_protein) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_last_4_protein
        FROM RankedData
    ),
    Differences AS (
        SELECT
            *,
            total_calories - avg_last_4_calories AS calories_diff,
            total_lipids - avg_last_4_lipids AS lipids_diff,
            total_carbs - avg_last_4_carbs AS carbs_diff,
            total_protein - avg_last_4_protein AS protein_diff,
            STDDEV_POP(total_calories) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS calories_std_dev,
            STDDEV_POP(total_lipids) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS lipids_std_dev,
            STDDEV_POP(total_carbs) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS carbs_std_dev,
            STDDEV_POP(total_protein) OVER(PARTITION BY user_id, day_of_week ORDER BY rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS protein_std_dev
        FROM WindowedAverages
    )
    SELECT *,
        CASE WHEN ABS(calories_diff) > 2 * calories_std_dev THEN 1 ELSE 0 END AS calories_outlier,
        CASE WHEN ABS(lipids_diff) > 2 * lipids_std_dev THEN 1 ELSE 0 END AS lipids_outlier,
        CASE WHEN ABS(carbs_diff) > 2 * carbs_std_dev THEN 1 ELSE 0 END AS carbs_outlier,
        CASE WHEN ABS(protein_diff) > 2 * protein_std_dev THEN 1 ELSE 0 END AS protein_outlier
    FROM Differences
    """

    # Exécuter la requête et récupérer les résultats
    result_df = conn.execute(query).df()

    # Sauvegarder les résultats dans un fichier Excel
    result_df.to_excel('data/duckdb_window_function_results.xlsx', index=False)

    return result_df

# Exécution de la fonction
if __name__ == "__main__":
    pandas_window_function()
    duckdb_window_function()