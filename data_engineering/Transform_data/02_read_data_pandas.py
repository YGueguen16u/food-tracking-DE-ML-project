import os
import time
import pandas as pd
import duckdb
from pyspark.sql import SparkSession


def pandas_read_data():
    start_time = time.time()
    df = pd.read_excel('combined_meal_data.xlsx')

    # Afficher un aperçu de la DataFrame consolidée
    print(df.head())

    # Analyse des individus
    unique_users = df['user_id'].nunique()
    avg_meals_per_user = df.groupby('user_id').size().mean()

    print(f"Nombre d'utilisateurs uniques : {unique_users}")
    print(f"Nombre moyen de repas par utilisateur : {avg_meals_per_user}")

    elapsed_time = time.time() - start_time
    print(f"Elapsed time : {elapsed_time}")

    return unique_users, avg_meals_per_user, elapsed_time


def duckdb_read_data():
    start_time = time.time()
    # Chemin vers le fichier Excel
    file_path = 'combined_meal_data.xlsx'

    # Lire la feuille Excel dans un DataFrame Pandas
    df_pandas = pd.read_excel(file_path, sheet_name='Sheet1')

    # Créer une connexion DuckDB en mémoire
    conn = duckdb.connect(database=':memory:')

    # Charger le DataFrame Pandas dans DuckDB
    conn.register('df_pandas', df_pandas)

    # Exécuter des requêtes DuckDB sur les données
    df = conn.execute("SELECT * FROM df_pandas").df()

    # Afficher un aperçu de la DataFrame
    print(df.head())

    # Analyse des individus
    unique_users = conn.execute("SELECT COUNT(DISTINCT user_id) FROM df_pandas").fetchone()[0]
    avg_meals_per_user = \
        conn.execute("SELECT AVG(count) FROM (SELECT COUNT(*) AS count FROM df_pandas GROUP BY user_id)").fetchone()[0]

    print(f"Nombre d'utilisateurs uniques : {unique_users}")
    print(f"Nombre moyen de repas par utilisateur : {avg_meals_per_user}")

    elapsed_time = time.time() - start_time
    print(f"Elapsed time : {elapsed_time}")

    return unique_users, avg_meals_per_user, elapsed_time


"""
def pyspark_read_data():
    start_time = time.time()
    # Créer une session Spark
    spark = SparkSession.builder.appName("ETL_Projet").getOrCreate()

    # Chemin vers le fichier Excel
    file_path = 'combined_meal_data.xlsx'

    # Lire la feuille Excel dans un DataFrame Pandas
    df_pandas = pd.read_excel(file_path, sheet_name='Sheet1')

    # Convertir le DataFrame Pandas en DataFrame PySpark
    df_spark = spark.createDataFrame(df_pandas)

    # Afficher un aperçu de la DataFrame
    df_spark.show()

    # Analyse des individus
    unique_users = df_spark.select("user_id").distinct().count()
    avg_meals_per_user = df_spark.groupBy("user_id").count().agg({"count": "avg"}).collect()[0][0]

    print(f"Nombre d'utilisateurs uniques : {unique_users}")
    print(f"Nombre moyen de repas par utilisateur : {avg_meals_per_user}")

    # Arrêter la session Spark
    spark.stop()

    elapsed_time = time.time() - start_time
    print(f"Elapsed time : {elapsed_time}")

    return unique_users, avg_meals_per_user, elapsed_time
"""


def compare_results():
    # Exécution des trois fonctions
    pandas_results = pandas_read_data()
    duckdb_results = duckdb_read_data()
    # pyspark_results = pyspark_read_data()

    # Comparaison des résultats
    print("\n--- Comparaison des résultats ---")
    assert pandas_results[:2] == duckdb_results[:2], "Les résultats sont différents entre les méthodes !"
    # assert pandas_results[:2] == duckdb_results[:2] == pyspark_results[:2], "Les résultats sont différents entre les méthodes !"

    print("Les résultats sont identiques entre toutes les méthodes.")

    # Comparaison des temps d'exécution
    times = {
        "Pandas": pandas_results[2],
        "DuckDB": duckdb_results[2],
        # "PySpark": pyspark_results[2]
    }

    fastest = min(times, key=times.get)
    print(f"\nLa méthode la plus rapide est : {fastest} avec un temps de {times[fastest]:.4f} secondes")


if __name__ == "__main__":
    compare_results()
