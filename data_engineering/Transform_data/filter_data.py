"""
Ce module contient des fonctions pour nettoyer et analyser les données de repas.
"""

import os

import pandas as pd


def clean_meal_data(
        input_file, output_file, threshold_calories_high=5000, threshold_quantity_high=50):
    """
    Nettoie les données de repas en supprimant les valeurs nulles et les unités aberrantes.
    Enregistre les lignes supprimées et les doublons.

    Args:
        input_file (str): Chemin vers le fichier d'entrée (.xlsx).
        output_file (str): Chemin vers le fichier de sortie filtré (.xlsx).
        threshold_calories_high (int): Seuil pour les calories par unité (par défaut : 5000).
        threshold_quantity_high (int): Seuil pour les quantités élevées (par défaut : 100).
    """

    # Chargement des données
    dataframe = pd.read_excel(input_file)

    # Journal pour les lignes supprimées
    log_deleted_rows = []

    # 1. Supprimer les enregistrements avec des valeurs nulles dans des colonnes critiques
    colonnes_critiques = [
        "aliment_id",
        "quantity",
        "Valeur calorique",
        "Lipides",
        "Glucides",
        "Protein",
    ]
    df_cleaned = dataframe.dropna(subset=colonnes_critiques)

    # Journaliser les lignes supprimées
    log_deleted_rows.append(dataframe[~dataframe.index.isin(df_cleaned.index)])

    # 2. Supprimer les enregistrements avec des unités aberrantes (comme des valeurs négatives)
    df_cleaned = df_cleaned[df_cleaned["Valeur calorique"] >= 0]

    # Journaliser les lignes supprimées pour cette étape
    log_deleted_rows.append(dataframe[~dataframe.index.isin(df_cleaned.index)])

    # 3. Contrôle des valeurs extrêmes pour les calories par quantité et pour les quantités élevées
    df_cleaned["extreme_quantity"] = df_cleaned.apply(
        lambda row: (
            "High"
            if row["Valeur calorique"] > threshold_calories_high
               or row["quantity"] > threshold_quantity_high
            else "Normal"
        ),
        axis=1,
    )

    # 4. Analyse des valeurs nulles ou aberrantes par utilisateur
    user_nulls = dataframe.isnull().groupby(dataframe["user_id"]).sum()
    user_aberrations = df_cleaned.groupby("user_id")["extreme_quantity"].apply(
        lambda x: (x != "Normal").sum()
    )

    # 5. Journalisation des lignes supprimées
    deleted_rows = pd.concat(log_deleted_rows).drop_duplicates()
    deleted_rows.to_csv("deleted_rows_log.csv", index=False)

    # 6. Vérification des doublons après nettoyage
    df_cleaned = df_cleaned.drop_duplicates()

    # Sauvegarde des données nettoyées
    output_directory = os.path.dirname(output_file)

    # Check if the output directory is non-empty and create it if needed
    if output_directory and not os.path.exists(output_directory):
        os.makedirs(output_directory)

    df_cleaned.to_excel(output_file, index=False)

    # Affichage des analyses
    print("Nombre de valeurs nulles par utilisateur :")
    print(user_nulls)
    print("\nNombre de valeurs aberrantes par utilisateur :")
    print(user_aberrations)


# Utilisation de la fonction
if __name__ == "__main__":
    clean_meal_data(
        input_file="data/combined_meal_data.xlsx",
        output_file="data/combined_meal_data_filtered.xlsx",
        threshold_calories_high=5000,
        threshold_quantity_high=100,
    )
