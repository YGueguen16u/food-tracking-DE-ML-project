import duckdb
import numpy as np
import pandas as pd


# Fonction Pandas
def pandas_window_function():
    # Charger les données à partir du fichier Excel
    df = pd.read_excel(
        "data/pandas_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
    )

    # Convertir la colonne 'date' en format datetime
    df["date"] = pd.to_datetime(df["date"])

    # Extraire le jour de la semaine pour chaque date sous forme numérique (alignement avec DuckDB)
    df["day_of_week"] = df["date"].dt.dayofweek + 1  # Lundi = 1, Dimanche = 7

    # Trier les données par user_id, day_of_week, puis par date
    df = df.sort_values(by=["user_id", "day_of_week", "date"])

    # Calcul des moyennes sur les 4 derniers jours similaires
    df["avg_last_4_calories"] = df.groupby(["user_id", "day_of_week"])[
        "total_calories"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df["avg_last_4_lipids"] = df.groupby(["user_id", "day_of_week"])[
        "total_lipids"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df["avg_last_4_carbs"] = df.groupby(["user_id", "day_of_week"])[
        "total_carbs"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    df["avg_last_4_protein"] = df.groupby(["user_id", "day_of_week"])[
        "total_protein"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

    # Calcul des différences par rapport à la moyenne des 4 derniers jours
    df["calories_diff"] = df["total_calories"] - df["avg_last_4_calories"]
    df["lipids_diff"] = df["total_lipids"] - df["avg_last_4_lipids"]
    df["carbs_diff"] = df["total_carbs"] - df["avg_last_4_carbs"]
    df["protein_diff"] = df["total_protein"] - df["avg_last_4_protein"]

    # Calcul des écarts-types pour les 4 derniers jours
    df["calories_std_dev"] = df.groupby(["user_id", "day_of_week"])[
        "total_calories"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    df["lipids_std_dev"] = df.groupby(["user_id", "day_of_week"])[
        "total_lipids"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    df["carbs_std_dev"] = df.groupby(["user_id", "day_of_week"])[
        "total_carbs"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    df["protein_std_dev"] = df.groupby(["user_id", "day_of_week"])[
        "total_protein"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())

    # Calcul des outliers avec la méthode IQR pour chaque colonne
    for col in ["total_calories", "total_lipids", "total_carbs", "total_protein"]:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 2 * IQR
        upper_bound = Q3 + 2 * IQR
        df[f"{col}_outlier"] = (df[col] < lower_bound) | (df[col] > upper_bound)

    # Arrondir les résultats à 3 décimales pour éviter les différences de précision
    df = df.round(3)

    # Sauvegarder les résultats dans un fichier Excel
    df.to_excel("data/pandas_window_function_results.xlsx", index=False)

    return df


# Fonction DuckDB
def duckdb_window_function():
    # Charger les données à partir du fichier Excel
    df = pd.read_excel(
        "data/duckdb_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
    )

    # Convertir la colonne 'date' en format datetime
    df["date"] = pd.to_datetime(df["date"])

    # Créer une connexion DuckDB en mémoire
    conn = duckdb.connect(database=":memory:")

    # Charger le DataFrame Pandas dans DuckDB
    conn.register("df", df)

    # Modifier l'extraction du jour de la semaine pour que Dimanche soit 7, Lundi soit 1, etc.
    conn.execute(
        """
    CREATE TABLE df_with_day_of_week AS
    SELECT *,

        CASE 
            WHEN STRFTIME('%w', date) = '0' THEN 7  -- Dimanche devient 7
            ELSE CAST(STRFTIME('%w', date) AS INTEGER)
        END AS day_of_week
    FROM df
    """
    )

    # Exécuter la requête pour calculer les fenêtres et les quartiles
    query = """
    WITH RankedData AS (
        SELECT 
            date,
            user_id,
            total_calories,
            total_lipids,
            total_carbs,
            total_protein,
            day_of_week,
            ROW_NUMBER() OVER(PARTITION BY user_id, day_of_week ORDER BY date DESC) AS rn
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
    Quartiles AS (
        SELECT 
            user_id, 
            day_of_week, 
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_calories) AS Q1_calories,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_calories) AS Q3_calories,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_lipids) AS Q1_lipids,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_lipids) AS Q3_lipids,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_carbs) AS Q1_carbs,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_carbs) AS Q3_carbs,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_protein) AS Q1_protein,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_protein) AS Q3_protein
        FROM df_with_day_of_week
        GROUP BY user_id, day_of_week
    )
    SELECT WindowedAverages.*,
           Quartiles.Q1_calories, Quartiles.Q3_calories, 
           Quartiles.Q1_lipids, Quartiles.Q3_lipids, 
           Quartiles.Q1_carbs, Quartiles.Q3_carbs, 
           Quartiles.Q1_protein, Quartiles.Q3_protein,
           (Quartiles.Q3_calories - Quartiles.Q1_calories) AS IQR_calories,
           (Quartiles.Q3_lipids - Quartiles.Q1_lipids) AS IQR_lipids,
           (Quartiles.Q3_carbs - Quartiles.Q1_carbs) AS IQR_carbs,
           (Quartiles.Q3_protein - Quartiles.Q1_protein) AS IQR_protein,
           CASE WHEN total_calories < Quartiles.Q1_calories - 2 * (Quartiles.Q3_calories - Quartiles.Q1_calories) OR 
                     total_calories > Quartiles.Q3_calories + 2 * (Quartiles.Q3_calories - Quartiles.Q1_calories) 
                THEN 1 ELSE 0 END AS calories_outlier,
           CASE WHEN total_lipids < Quartiles.Q1_lipids - 2 * (Quartiles.Q3_lipids - Quartiles.Q1_lipids) OR 
                     total_lipids > Quartiles.Q3_lipids + 2 * (Quartiles.Q3_lipids - Quartiles.Q1_lipids) 
                THEN 1 ELSE 0 END AS lipids_outlier,
           CASE WHEN total_carbs < Quartiles.Q1_carbs - 2 * (Quartiles.Q3_carbs - Quartiles.Q1_carbs) OR 
                     total_carbs > Quartiles.Q3_carbs + 2 * (Quartiles.Q3_carbs - Quartiles.Q1_carbs) 
                THEN 1 ELSE 0 END AS carbs_outlier,
           CASE WHEN total_protein < Quartiles.Q1_protein - 2 * (Quartiles.Q3_protein - Quartiles.Q1_protein) OR 
                     total_protein > Quartiles.Q3_protein + 2 * (Quartiles.Q3_protein - Quartiles.Q1_protein) 
                THEN 1 ELSE 0 END AS protein_outlier
    FROM WindowedAverages
    JOIN Quartiles ON WindowedAverages.user_id = Quartiles.user_id
                    AND WindowedAverages.day_of_week = Quartiles.day_of_week
    """

    # Exécuter la requête et récupérer les résultats
    result_df = conn.execute(query).df()

    # Sauvegarder les résultats dans un fichier Excel
    result_df.to_excel("data/duckdb_window_function_results_iqr.xlsx", index=False)

    return result_df


# Exécution des fonctions
if __name__ == "__main__":
    pandas_window_function()
    duckdb_window_function()
