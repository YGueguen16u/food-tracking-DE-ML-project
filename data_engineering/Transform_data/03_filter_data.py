import os
import pandas as pd

# Chargement des données (remplace 'combined_meal_data.xlsx' par le chemin vers ton fichier)
df = pd.read_excel('data/combined_meal_data.xlsx')

# Journal pour les lignes supprimées
log_deleted_rows = []

# 1. Supprimer les enregistrements avec des valeurs nulles dans des colonnes critiques
colonnes_critiques = ['aliment_id', 'quantity', 'Valeur calorique', 'Lipides', 'Glucides', 'Protein']
df_cleaned = df.dropna(subset=colonnes_critiques)

# Journaliser les lignes supprimées
log_deleted_rows.append(df[~df.index.isin(df_cleaned.index)])

# 2. Supprimer les enregistrements avec des unités aberrantes (par exemple, des valeurs négatives)
df_cleaned = df_cleaned[df_cleaned['Valeur calorique'] >= 0]

# Journaliser les lignes supprimées pour cette étape
log_deleted_rows.append(df[~df.index.isin(df_cleaned.index)])

# 3. Contrôle des valeurs extrêmes pour les calories par quantité et pour les quantités élevées
threshold_calories_high = 5000  # seuil pour les calories par unité
threshold_quantity_high = 100  # seuil pour la quantité

# Marquer les lignes avec des calories par unité trop élevées ou des quantités anormalement élevées
df_cleaned['extreme_quantity'] = df_cleaned.apply(
    lambda row: 'High' if row['Valeur calorique'] > threshold_calories_high or row['quantity'] > threshold_quantity_high else 'Normal',
    axis=1
)

# 4. Analyse des valeurs nulles ou aberrantes par utilisateur
user_nulls = df.isnull().groupby(df['user_id']).sum()
user_aberrations = df_cleaned.groupby('user_id')['extreme_quantity'].apply(lambda x: (x != 'Normal').sum())

# 5. Journalisation des lignes supprimées
deleted_rows = pd.concat(log_deleted_rows).drop_duplicates()
deleted_rows.to_csv('deleted_rows_log.csv', index=False)

# 6. Vérification des doublons après nettoyage
df_cleaned = df_cleaned.drop_duplicates()

# Vérifier si le dossier 'data' existe, sinon le créer
output_directory = 'data'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Sauvegarde des données nettoyées
df_cleaned.to_excel(os.path.join(output_directory,'combined_meal_data_filtered.xlsx'), index=False)

# Affichage des analyses
print("Nombre de valeurs nulles par utilisateur :")
print(user_nulls)
print("\nNombre de valeurs aberrantes par utilisateur :")
print(user_aberrations)
