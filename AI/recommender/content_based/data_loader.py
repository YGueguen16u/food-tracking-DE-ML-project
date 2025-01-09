"""
Data loader spécifique pour le recommandeur basé sur le contenu.
"""
import pandas as pd
import numpy as np
import io
from aws_s3.connect_s3 import S3Manager

class ContentBasedDataLoader:
    """Classe pour charger les données d'entraînement pour le recommandeur basé sur le contenu"""
    
    def __init__(self):
        """Initialise la connexion S3"""
        self.s3_manager = S3Manager()
        
    def load_dataframe_from_s3(self, s3_key):
        """
        Charge un DataFrame depuis S3
        
        Args:
            s3_key (str): Chemin du fichier dans S3
            
        Returns:
            pd.DataFrame: DataFrame chargé
        """
        try:
            print(f"Chargement de {s3_key}...")
            response = self.s3_manager.s3_client.get_object(
                Bucket=self.s3_manager.bucket_name, 
                Key=s3_key
            )
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
            print(f"Données chargées : {len(df)} lignes")
            print("Colonnes :", df.columns.tolist())
            return df
        except Exception as e:
            print(f"Erreur lors du chargement de {s3_key}: {str(e)}")
            return None
    
    def clean_numeric_value(self, value):
        """Nettoie une valeur numérique"""
        if pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        
        try:
            # Remplacer la virgule par un point
            value = str(value).replace(',', '.')
            # Garder uniquement le dernier point
            parts = value.split('.')
            if len(parts) > 2:
                value = parts[0] + '.' + ''.join(parts[1:])
            return float(value)
        except:
            return 0.0
        
    def load_training_data(self):
        """
        Charge les données pour l'entraînement du recommandeur basé sur le contenu
        
        Returns:
            tuple: (données d'entraînement, données de test)
        """
        try:
            print("Début du chargement des données...")
            
            # Charger les données combinées filtrées
            combined_data = self.load_dataframe_from_s3('transform/folder_2_filter_data/combined_meal_data_filtered.xlsx')
            if combined_data is None:
                print("Erreur : impossible de charger les données combinées")
                return None, None
                
            # Charger les données de type d'aliments
            food_types = self.load_dataframe_from_s3('reference_data/food/food_processed.xlsx')
            if food_types is None:
                print("Erreur : impossible de charger les données des aliments")
                return None, None
                
            print("\nPréparation des préférences utilisateurs...")
            # Calculer un rating basé sur la fréquence de consommation
            user_aliment_counts = combined_data.groupby(['user_id', 'aliment_id']).size().reset_index(name='count')
            max_count = user_aliment_counts['count'].max()
            user_aliment_counts['rating'] = user_aliment_counts['count'] / max_count * 5  # Normaliser entre 0 et 5
            
            user_preferences = pd.DataFrame({
                'user_id': user_aliment_counts['user_id'],
                'id': user_aliment_counts['aliment_id'],
                'rating': user_aliment_counts['rating']
            })
            
            print("\nPréparation des features des aliments...")
            # Préparer les features des aliments
            food_features = pd.DataFrame({
                'id': food_types['id'],
                'Type': food_types['Type'],
                'Nom': food_types['Aliment'],
                'calories': food_types['Valeur calorique'].apply(self.clean_numeric_value),
                'lipides': food_types['Lipides'].apply(self.clean_numeric_value),
                'glucides': food_types['Glucides'].apply(self.clean_numeric_value),
                'proteines': food_types['Protein'].apply(self.clean_numeric_value),
                'fibres': food_types['Fibre alimentaire'].apply(self.clean_numeric_value),
                'sucres': food_types['Sucre'].apply(self.clean_numeric_value),
                'sodium': food_types['Sodium'].apply(self.clean_numeric_value)
            })
            
            print("\nNettoyage des valeurs numériques...")
            # Vérifier que toutes les valeurs sont numériques
            numeric_cols = ['calories', 'lipides', 'glucides', 'proteines', 'fibres', 'sucres', 'sodium']
            for col in numeric_cols:
                # Convertir en float et remplacer les NaN par 0
                food_features[col] = pd.to_numeric(food_features[col], errors='coerce').fillna(0)
                
            print("\nStatistiques des features après nettoyage:")
            print(food_features[numeric_cols].describe())
            
            print("\nDonnées chargées avec succès!")
            print(f"Nombre d'aliments : {len(food_features)}")
            print(f"Nombre d'utilisateurs : {len(user_preferences['user_id'].unique())}")
            print(f"Nombre total d'interactions : {len(user_preferences)}")
            
            return food_features, user_preferences
            
        except Exception as e:
            print(f"Erreur lors du chargement des données : {str(e)}")
            return None, None
