"""
This script processes user dietary data and calculates moving averages,
differences, and identifies outliers based on the Interquartile Range (IQR).
The script uses both Pandas (for Python) and DuckDB (for SQL-based operations).
"""

import duckdb
import pandas as pd


# Fonction Pandas
def pandas_window_function():
    """
    Processes the data using Pandas to calculate moving averages for
    total calories, lipids, carbs, and protein over the last four similar days
    (same day of the week), calculates the difference from the average,
    and identifies outliers using the Interquartile Range (IQR) method.

    Outliers are identified if they fall below Q1 - 2 * IQR or above Q3 + 2 * IQR.
    The processed data is saved to an Excel file.

    Returns:
        DataFrame: The processed data with outlier flags.
    """
    # Charger les données à partir du fichier Excel
    data_frame = pd.read_excel(
        "data/pandas_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
    )

    # Convertir la colonne 'date' en format datetime
    data_frame["date"] = pd.to_datetime(data_frame["date"])

    # Extraire le jour de la semaine pour chaque date sous forme numérique (alignement avec DuckDB)
    data_frame["day_of_week"] = (
        data_frame["date"].dt.dayofweek + 1
    )  # Lundi = 1, Dimanche = 7

    # Trier les données par user_id, day_of_week, puis par date
    data_frame = data_frame.sort_values(by=["user_id", "day_of_week", "date"])

    # Calcul des moyennes sur les 4 derniers jours similaires
    data_frame["avg_last_4_calories"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_calories"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    data_frame["avg_last_4_lipids"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_lipids"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    data_frame["avg_last_4_carbs"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_carbs"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())
    data_frame["avg_last_4_protein"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_protein"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).mean())

    # Calcul des différences par rapport à la moyenne des 4 derniers jours
    data_frame["calories_diff"] = (
        data_frame["total_calories"] - data_frame["avg_last_4_calories"]
    )
    data_frame["lipids_diff"] = (
        data_frame["total_lipids"] - data_frame["avg_last_4_lipids"]
    )
    data_frame["carbs_diff"] = (
        data_frame["total_carbs"] - data_frame["avg_last_4_carbs"]
    )
    data_frame["protein_diff"] = (
        data_frame["total_protein"] - data_frame["avg_last_4_protein"]
    )

    # Calcul des écarts-types pour les 4 derniers jours
    data_frame["calories_std_dev"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_calories"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    data_frame["lipids_std_dev"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_lipids"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    data_frame["carbs_std_dev"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_carbs"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())
    data_frame["protein_std_dev"] = data_frame.groupby(["user_id", "day_of_week"])[
        "total_protein"
    ].transform(lambda x: x.rolling(window=4, min_periods=1).std())

    # Calcul des outliers avec la méthode IQR pour chaque colonne
    for col in ["total_calories", "total_lipids", "total_carbs", "total_protein"]:
        grouped = data_frame.groupby(["user_id", "day_of_week"])[col]
        quartile_1 = grouped.transform(lambda x: x.quantile(0.25))
        quartile_3 = grouped.transform(lambda x: x.quantile(0.75))
        iqr = quartile_3 - quartile_1
        lower_bound = quartile_1 - 2 * iqr
        upper_bound = quartile_3 + 2 * iqr
        data_frame[f"{col}_outlier"] = (
                (data_frame[col] < lower_bound) | (data_frame[col] > upper_bound)
        ).astype(int)

    # Arrondir les résultats à 3 décimales pour éviter les différences de précision
    data_frame = data_frame.round(3)

    # Sauvegarder les résultats dans un fichier Excel
    data_frame.to_excel("data/pandas_window_function_results_iqr.xlsx", index=False)

    return data_frame


# Fonction DuckDB
def duckdb_window_function():
    """
    Processes the data using DuckDB to calculate moving averages and
    identify outliers using the Interquartile Range (IQR) method.

    The function computes quartiles (Q1 and Q3) and identifies outliers
    for total calories, lipids, carbs, and protein based on the IQR method.

    Outliers are flagged if they fall outside the range [Q1 - 2 * IQR, Q3 + 2 * IQR].
    The processed data is saved to an Excel file.

    Returns:
        DataFrame: The processed data with calculated IQR outlier flags.
    """
    # Charger les données à partir du fichier Excel
    data_frame = pd.read_excel(
        "data/duckdb_aggregation_results.xlsx", sheet_name="User Daily Aggregation"
    )

    # Convertir la colonne 'date' en format datetime
    data_frame["date"] = pd.to_datetime(data_frame["date"])

    # Créer une connexion DuckDB en mémoire
    conn = duckdb.connect(database=":memory:")

    # Charger le DataFrame Pandas dans DuckDB
    conn.register("df", data_frame)

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
                THEN 1 ELSE 0 END AS total_calories_outlier,
           CASE WHEN total_lipids < Quartiles.Q1_lipids - 2 * (Quartiles.Q3_lipids - Quartiles.Q1_lipids) OR 
                     total_lipids > Quartiles.Q3_lipids + 2 * (Quartiles.Q3_lipids - Quartiles.Q1_lipids) 
                THEN 1 ELSE 0 END AS total_lipids_outlier,
           CASE WHEN total_carbs < Quartiles.Q1_carbs - 2 * (Quartiles.Q3_carbs - Quartiles.Q1_carbs) OR 
                     total_carbs > Quartiles.Q3_carbs + 2 * (Quartiles.Q3_carbs - Quartiles.Q1_carbs) 
                THEN 1 ELSE 0 END AS total_carbs_outlier,
           CASE WHEN total_protein < Quartiles.Q1_protein - 2 * (Quartiles.Q3_protein - Quartiles.Q1_protein) OR 
                     total_protein > Quartiles.Q3_protein + 2 * (Quartiles.Q3_protein - Quartiles.Q1_protein) 
                THEN 1 ELSE 0 END AS total_protein_outlier
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
