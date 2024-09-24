import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import time

def pandas_group_data():
    # Code inchangé (le même que celui que vous avez déjà)
    start_time = time.time()

    # Charger les données
    df = pd.read_excel('data/combined_meal_data_filtered.xlsx')

    # Convertir la colonne 'date' en format datetime
    df['date'] = pd.to_datetime(df['date'])

    # Compter le nombre d'utilisateurs distincts par date
    distinct_users_per_day = df.groupby('date')['user_id'].nunique().reset_index()
    distinct_users_per_day.columns = ['date', 'distinct_user_count']

    # Agréger les données par jour
    daily_aggregation = df.groupby('date').agg({
        'total_calories': 'sum',
        'total_lipids': 'sum',
        'total_carbs': 'sum',
        'total_protein': 'sum'
    }).reset_index()

    # Diviser chaque total par le nombre d'utilisateurs distincts
    daily_aggregation = pd.merge(daily_aggregation, distinct_users_per_day, on='date')
    daily_aggregation['total_calories'] /= daily_aggregation['distinct_user_count']
    daily_aggregation['total_lipids'] /= daily_aggregation['distinct_user_count']
    daily_aggregation['total_carbs'] /= daily_aggregation['distinct_user_count']
    daily_aggregation['total_protein'] /= daily_aggregation['distinct_user_count']

    # 2. Agréger les données par jour et par utilisateur
    user_daily_aggregation = df.groupby(['date', 'user_id']).agg({
        'total_calories': 'sum',
        'total_lipids': 'sum',
        'total_carbs': 'sum',
        'total_protein': 'sum'
    }).reset_index()

    # 3. Calcul de la consommation moyenne par aliment par jour
    daily_mean_per_food = df.groupby(['date', 'Aliment']).agg({
        'total_calories': 'mean',
        'total_lipids': 'mean',
        'total_carbs': 'mean',
        'total_protein': 'mean'
    }).reset_index()

    # 4. Visualisation des variations des nutriments au fil du temps
    plt.figure(figsize=(10, 6))
    daily_aggregation.plot(x='date', y=['total_calories', 'total_lipids', 'total_carbs', 'total_protein'], kind='line')
    plt.title('Variations des nutriments consommés au fil du temps')
    plt.xlabel('Date')
    plt.ylabel('Quantité consommée en moyenne')
    plt.show()

    # 5. Groupement pour analyser la répartition des nutriments par utilisateur et par type d’aliment
    user_food_grouping = df.groupby(['user_id', 'Aliment']).agg({
        'total_calories': 'sum',
        'total_lipids': 'sum',
        'total_carbs': 'sum',
        'total_protein': 'sum'
    }).reset_index()

    elapsed_time = time.time() - start_time

    # Sauvegarder les résultats dans un fichier Excel
    with pd.ExcelWriter('data/pandas_aggregation_results.xlsx') as writer:
        daily_aggregation.to_excel(writer, sheet_name='Daily Aggregation', index=False)
        user_daily_aggregation.to_excel(writer, sheet_name='User Daily Aggregation', index=False)
        daily_mean_per_food.to_excel(writer, sheet_name='Daily Mean Per Food', index=False)
        user_food_grouping.to_excel(writer, sheet_name='User Food Grouping', index=False)

    return {
        'daily_aggregation': daily_aggregation,
        'user_daily_aggregation': user_daily_aggregation,
        'daily_mean_per_food': daily_mean_per_food,
        'user_food_grouping': user_food_grouping,
        'elapsed_time': elapsed_time
    }


def duckdb_group_data():
    # Code inchangé (le même que celui que vous avez déjà)
    start_time = time.time()

    # Charger les données
    df = pd.read_excel('data/combined_meal_data.xlsx')

    # Créer une connexion DuckDB en mémoire
    conn = duckdb.connect(database=':memory:')

    # Charger le DataFrame Pandas dans DuckDB
    conn.register('df', df)

    # Exécuter une requête pour obtenir l'agrégation divisée par le nombre d'utilisateurs distincts
    daily_aggregation = conn.execute("""
        SELECT 
            date,
            SUM(total_calories) / COUNT(DISTINCT user_id) AS avg_calories_per_user,
            SUM(total_lipids) / COUNT(DISTINCT user_id) AS avg_lipids_per_user,
            SUM(total_carbs) / COUNT(DISTINCT user_id) AS avg_carbs_per_user,
            SUM(total_protein) / COUNT(DISTINCT user_id) AS avg_protein_per_user
        FROM df
        GROUP BY date
        ORDER BY date
    """).df()

    # 2. Agréger les données par jour et par utilisateur
    user_daily_aggregation = conn.execute("""
        SELECT date,
               user_id,
               SUM(total_calories) AS total_calories,
               SUM(total_lipids) AS total_lipids,
               SUM(total_carbs) AS total_carbs,
               SUM(total_protein) AS total_protein
        FROM df
        GROUP BY date, user_id
    """).df()

    # 3. Calcul de la consommation moyenne par aliment par jour
    daily_mean_per_food = conn.execute("""
        SELECT date,
               Aliment,
               AVG(total_calories) AS avg_calories,
               AVG(total_lipids) AS avg_lipids,
               AVG(total_carbs) AS avg_carbs,
               AVG(total_protein) AS avg_protein
        FROM df
        WHERE Aliment IS NOT NULL  -- Filter out NULL Aliment values
        GROUP BY date, Aliment
    """).df()

    # 4. Exporter les résultats vers un DataFrame Pandas pour la visualisation
    daily_aggregation.plot(x='date', y=['avg_calories_per_user', 'avg_lipids_per_user', 'avg_carbs_per_user', 'avg_protein_per_user'], kind='line')
    plt.title('Variations des nutriments consommés au fil du temps')
    plt.xlabel('Date')
    plt.ylabel('Quantité consommée en moyenne')
    plt.show()

    # 5. Groupement pour analyser la répartition des nutriments par utilisateur et par type d’aliment
    user_food_grouping = conn.execute("""
        SELECT user_id,
               Aliment,
               SUM(total_calories) AS total_calories,
               SUM(total_lipids) AS total_lipids,
               SUM(total_carbs) AS total_carbs,
               SUM(total_protein) AS total_protein
        FROM df
        WHERE Aliment IS NOT NULL  -- Filter out NULL Aliment values
        GROUP BY user_id, Aliment
    """).df()

    elapsed_time = time.time() - start_time

    # Sauvegarder les résultats dans un fichier Excel
    with pd.ExcelWriter('data/duckdb_aggregation_results.xlsx') as writer:
        daily_aggregation.to_excel(writer, sheet_name='Daily Aggregation', index=False)
        user_daily_aggregation.to_excel(writer, sheet_name='User Daily Aggregation', index=False)
        daily_mean_per_food.to_excel(writer, sheet_name='Daily Mean Per Food', index=False)
        user_food_grouping.to_excel(writer, sheet_name='User Food Grouping', index=False)

    return {
        'daily_aggregation': daily_aggregation,
        'user_daily_aggregation': user_daily_aggregation,
        'daily_mean_per_food': daily_mean_per_food,
        'user_food_grouping': user_food_grouping,
        'elapsed_time': elapsed_time
    }


def compare_results():
    """
    Compare les résultats obtenus par les méthodes Pandas et DuckDB.

    Cette fonction :
    - Exécute les deux méthodes et compare les résultats par clé de regroupement.
    - Vérifie si les résultats sont identiques pour les mêmes clés.
    - Compare les temps d'exécution et affiche la méthode la plus rapide.
    - Écrit les résultats dans un fichier texte.

    Returns:
        None
    """
    pandas_results = pandas_group_data()
    duckdb_results = duckdb_group_data()

    with open("data/comparison_results.txt", "w") as f:
        # Comparaison pour `daily_aggregation`
        merged_df = pandas_results['daily_aggregation'].merge(
            duckdb_results['daily_aggregation'],
            left_on='date', right_on='date',
            suffixes=('_pandas', '_duckdb'),
            how='outer',
            indicator=True
        )
        differences = merged_df[merged_df['_merge'] != 'both']

        if differences.empty:
            result = "Les résultats pour daily_aggregation sont identiques.\n"
        else:
            result = "Les résultats pour daily_aggregation sont différents.\n"
            result += f"Differences found:\n{differences}\n"
        print(result)
        f.write(result)

        # Comparaison pour `user_daily_aggregation`
        merged_df = pandas_results['user_daily_aggregation'].merge(
            duckdb_results['user_daily_aggregation'],
            left_on=['date', 'user_id'], right_on=['date', 'user_id'],
            suffixes=('_pandas', '_duckdb'),
            how='outer',
            indicator=True
        )
        differences = merged_df[merged_df['_merge'] != 'both']

        if differences.empty:
            result = "Les résultats pour user_daily_aggregation sont identiques.\n"
        else:
            result = "Les résultats pour user_daily_aggregation sont différents.\n"
            result += f"Differences found:\n{differences}\n"
        print(result)
        f.write(result)

        # Comparaison pour `daily_mean_per_food`
        merged_df = pandas_results['daily_mean_per_food'].merge(
            duckdb_results['daily_mean_per_food'],
            left_on=['date', 'Aliment'], right_on=['date', 'Aliment'],
            suffixes=('_pandas', '_duckdb'),
            how='outer',
            indicator=True
        )
        differences = merged_df[merged_df['_merge'] != 'both']
        differences.to_csv("data/differences_daily_mean_per_food.csv", index=False)

        if differences.empty:
            result = "Les résultats pour daily_mean_per_food sont identiques.\n"
        else:
            result = "Les résultats pour daily_mean_per_food sont différents.\n"
            result += f"Differences found:\n{differences}\n"
        print(result)
        f.write(result)
        # Sauvegarde des différences pour `daily_mean_per_food`

        # Comparaison pour `user_food_grouping`
        merged_df = pandas_results['user_food_grouping'].merge(
            duckdb_results['user_food_grouping'],
            left_on=['user_id', 'Aliment'], right_on=['user_id', 'Aliment'],
            suffixes=('_pandas', '_duckdb'),
            how='outer',
            indicator=True
        )
        differences = merged_df[merged_df['_merge'] != 'both']
        differences.to_csv("data/differences_food_grouping.csv", index=False)

        if differences.empty:
            result = "Les résultats pour user_food_grouping sont identiques.\n"
        else:
            result = "Les résultats pour user_food_grouping sont différents.\n"
            result += f"Differences found:\n{differences}\n"
        print(result)
        f.write(result)

        # Comparer les temps d'exécution
        time_results = (
            f"\nTemps d'exécution :\n"
            f"Pandas : {pandas_results['elapsed_time']:.4f} secondes\n"
            f"DuckDB : {duckdb_results['elapsed_time']:.4f} secondes\n"
        )
        print(time_results)
        f.write(time_results)

        if pandas_results['elapsed_time'] < duckdb_results['elapsed_time']:
            speed_result = "\nPandas est plus rapide.\n"
        else:
            speed_result = "\nDuckDB est plus rapide.\n"
        print(speed_result)
        f.write(speed_result)

def display_result():
    """
    Affiche et enregistre les résultats des méthodes Pandas et DuckDB.

    Cette fonction affiche les résultats pour les deux méthodes et les enregistre
    également dans un fichier texte pour comparaison.

    Returns:
        None
    """
    pandas_results = pandas_group_data()
    duckdb_results = duckdb_group_data()

    with open("data/display_results.txt", "w") as f:
        pandas_str = f"Pandas Results:\n{pandas_results}\n\n"
        duckdb_str = f"DuckDB Results:\n{duckdb_results}\n\n"

        print(pandas_str)
        print(duckdb_str)

        f.write(pandas_str)
        f.write(duckdb_str)

if __name__ == "__main__":
    compare_results()
    display_result()
