import pandas as pd
import numpy as np
import duckdb
import os


def pandas_percentage_change(df):
    # Calcul du pourcentage de changement et des alertes
    df['pct_change_calories'] = np.where(df['avg_last_4_calories'] != 0,
                                         ((df['total_calories'] - df['avg_last_4_calories']) / df['avg_last_4_calories']) * 100, 0)
    df['pct_change_lipids'] = np.where(df['avg_last_4_lipids'] != 0,
                                       ((df['total_lipids'] - df['avg_last_4_lipids']) / df['avg_last_4_lipids']) * 100, 0)
    df['pct_change_carbs'] = np.where(df['avg_last_4_carbs'] != 0,
                                      ((df['total_carbs'] - df['avg_last_4_carbs']) / df['avg_last_4_carbs']) * 100, 0)
    df['pct_change_protein'] = np.where(df['avg_last_4_protein'] != 0,
                                        ((df['total_protein'] - df['avg_last_4_protein']) / df['avg_last_4_protein']) * 100, 0)

    # Calcul des alertes pour chaque nutriment
    for nutrient in ['calories', 'lipids', 'carbs', 'protein']:
        df[f'{nutrient}_alert'] = np.abs(df[f'pct_change_{nutrient}']) > 20

    # Score de cohérence alimentaire (moyenne des alertes pour chaque user_id)
    df['consistency_score'] = df[[f'{nutrient}_alert' for nutrient in ['calories', 'lipids', 'carbs', 'protein']]].mean(axis=1)

    # Normaliser le score de cohérence par user_id
    df['user_count'] = df.groupby('user_id')['user_id'].transform('count')
    df['normalized_consistency_score'] = 100*df['consistency_score'] / df['user_count']

    return df


def user_comparison(df):
    # Agrégation des scores moyens par utilisateur
    return df.groupby('user_id').agg({
        'pct_change_calories': 'mean',
        'pct_change_lipids': 'mean',
        'pct_change_carbs': 'mean',
        'pct_change_protein': 'mean',
        'normalized_consistency_score': 'mean'
    }).sort_values(by='normalized_consistency_score', ascending=True)


def duckdb_percentage_change():
    # Charger les données
    df = pd.read_excel('data/duckdb_window_function_results_iqr.xlsx')

    # Connexion DuckDB avec calculs intégrés
    query = """
        WITH PercentageChanges AS (
            SELECT *,
                ((total_calories - avg_last_4_calories) / NULLIF(avg_last_4_calories, 0)) * 100 AS pct_change_calories,
                ((total_lipids - avg_last_4_lipids) / NULLIF(avg_last_4_lipids, 0)) * 100 AS pct_change_lipids,
                ((total_carbs - avg_last_4_carbs) / NULLIF(avg_last_4_carbs, 0)) * 100 AS pct_change_carbs,
                ((total_protein - avg_last_4_protein) / NULLIF(avg_last_4_protein, 0)) * 100 AS pct_change_protein,

                CASE WHEN ABS(((total_calories - avg_last_4_calories) / NULLIF(avg_last_4_calories, 0)) * 100) > 20 THEN 1 ELSE 0 END AS calories_alert,
                CASE WHEN ABS(((total_lipids - avg_last_4_lipids) / NULLIF(avg_last_4_lipids, 0)) * 100) > 20 THEN 1 ELSE 0 END AS lipids_alert,
                CASE WHEN ABS(((total_carbs - avg_last_4_carbs) / NULLIF(avg_last_4_carbs, 0)) * 100) > 20 THEN 1 ELSE 0 END AS carbs_alert,
                CASE WHEN ABS(((total_protein - avg_last_4_protein) / NULLIF(avg_last_4_protein, 0)) * 100) > 20 THEN 1 ELSE 0 END AS protein_alert,

                ((CASE WHEN ABS(((total_calories - avg_last_4_calories) / NULLIF(avg_last_4_calories, 0)) * 100) > 20 THEN 1 ELSE 0 END +
                  CASE WHEN ABS(((total_lipids - avg_last_4_lipids) / NULLIF(avg_last_4_lipids, 0)) * 100) > 20 THEN 1 ELSE 0 END +
                  CASE WHEN ABS(((total_carbs - avg_last_4_carbs) / NULLIF(avg_last_4_carbs, 0)) * 100) > 20 THEN 1 ELSE 0 END +
                  CASE WHEN ABS(((total_protein - avg_last_4_protein) / NULLIF(avg_last_4_protein, 0)) * 100) > 20 THEN 1 ELSE 0 END
                ) / 4.0) AS consistency_score

            FROM df
        ),
        NormalizedScores AS (
            SELECT *,
                COUNT(user_id) OVER (PARTITION BY user_id) AS user_count,
                consistency_score / COUNT(user_id) OVER (PARTITION BY user_id) AS normalized_consistency_score
            FROM PercentageChanges
        )
        SELECT * FROM NormalizedScores
    """
    conn = duckdb.connect(database=':memory:')
    conn.register('df', df)
    result_df = conn.execute(query).df()

    # Comparaison des utilisateurs dans DuckDB
    user_comparison_df = conn.execute("""
        SELECT user_id,
            AVG(pct_change_calories) AS avg_pct_change_calories,
            AVG(pct_change_lipids) AS avg_pct_change_lipids,
            AVG(pct_change_carbs) AS avg_pct_change_carbs,
            AVG(pct_change_protein) AS avg_pct_change_protein,
            AVG(normalized_consistency_score) AS avg_consistency_score
        FROM result_df
        GROUP BY user_id
        ORDER BY avg_consistency_score ASC
    """).df()

    return result_df, user_comparison_df


if __name__ == "__main__":
    # Charger les données pour Pandas
    df = pd.read_excel('data/pandas_window_function_results_iqr.xlsx')
    processed_df = pandas_percentage_change(df)
    user_comparison_df = user_comparison(processed_df)

    # Sauvegarder dans un fichier Excel unique avec plusieurs feuilles pour Pandas
    output_dir = 'data/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with pd.ExcelWriter(output_dir + 'pandas_percentage_results.xlsx') as writer:
        processed_df.to_excel(writer, sheet_name='Percentage Changes', index=False)
        user_comparison_df.to_excel(writer, sheet_name='User Comparison', index=True)

    # Exécuter et sauvegarder les résultats de DuckDB
    result_df, duck_user_comparison_df = duckdb_percentage_change()
    with pd.ExcelWriter(output_dir + 'duckdb_percentage_results.xlsx') as writer:
        result_df.to_excel(writer, sheet_name='Percentage Changes', index=False)
        duck_user_comparison_df.to_excel(writer, sheet_name='User Comparison', index=True)
