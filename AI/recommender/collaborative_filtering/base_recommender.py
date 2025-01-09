import os
import sys
import io
import tempfile

# Ajouter le chemin racine au PYTHONPATH
root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(root_path)

from aws_s3.connect_s3 import S3Manager
import pandas as pd
import numpy as np
from scipy.spatial.distance import cosine
from typing import List, Dict, Tuple
from datetime import datetime

class BaseRecommender:
    """Classe de base pour les systèmes de recommandation"""
    
    def __init__(self):
        """Initialise le système de recommandation"""
        self.s3_manager = S3Manager()
        self.ratings_matrix = None
        self.meal_features = None
        self.user_preferences = None
        
    def load_dataframe_from_s3(self, s3_key):
        """
        Charge un DataFrame depuis S3
        
        Args:
            s3_key (str): Chemin du fichier dans S3
            
        Returns:
            pd.DataFrame: DataFrame chargé
        """
        try:
            response = self.s3_manager.s3_client.get_object(
                Bucket=self.s3_manager.bucket_name, 
                Key=s3_key
            )
            return pd.read_excel(io.BytesIO(response['Body'].read()))
        except Exception as e:
            print(f"Erreur lors du chargement de {s3_key}: {str(e)}")
            return None

    def calculate_implicit_ratings(self, data):
        """
        Calcule les ratings implicites basés sur les habitudes alimentaires
        
        Args:
            data (pd.DataFrame): DataFrame contenant les données des repas
            
        Returns:
            pd.DataFrame: DataFrame avec les ratings calculés
        """
        # Convertir les colonnes en numérique
        numeric_columns = ['total_calories', 'total_protein', 'total_lipids', 'total_carbs']
        for col in numeric_columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')
        
        # Grouper par utilisateur et repas pour avoir des statistiques
        meal_stats = data.groupby(['user_id', 'meal_id']).agg({
            'total_calories': ['mean', 'count'],
            'total_protein': 'mean',
            'total_lipids': 'mean',
            'total_carbs': 'mean'
        }).reset_index()
        
        # Aplatir les colonnes multi-index
        meal_stats.columns = ['user_id', 'meal_id', 'avg_calories', 'meal_count', 
                            'avg_protein', 'avg_lipids', 'avg_carbs']
        
        # Calculer les scores nutritionnels (1-5)
        def calculate_nutrition_score(row):
            # Ratios recommandés approximatifs
            protein_ratio = row['avg_protein'] * 4 / row['avg_calories'] if row['avg_calories'] > 0 else 0
            lipids_ratio = row['avg_lipids'] * 9 / row['avg_calories'] if row['avg_calories'] > 0 else 0
            carbs_ratio = row['avg_carbs'] * 4 / row['avg_calories'] if row['avg_calories'] > 0 else 0
            
            # Score basé sur l'équilibre des macronutriments
            ideal_protein_ratio = 0.25  # 25% des calories
            ideal_lipids_ratio = 0.30   # 30% des calories
            ideal_carbs_ratio = 0.45    # 45% des calories
            
            protein_score = 5 - abs(protein_ratio - ideal_protein_ratio) * 10
            lipids_score = 5 - abs(lipids_ratio - ideal_lipids_ratio) * 10
            carbs_score = 5 - abs(carbs_ratio - ideal_carbs_ratio) * 10
            
            return np.mean([protein_score, lipids_score, carbs_score])
        
        # Calculer le score nutritionnel
        meal_stats['nutrition_score'] = meal_stats.apply(calculate_nutrition_score, axis=1)
        
        # Normaliser le nombre de repas (1-5)
        meal_stats['frequency_score'] = meal_stats['meal_count'] / meal_stats['meal_count'].max() * 4 + 1
        
        # Calculer le rating final (moyenne pondérée)
        meal_stats['rating'] = 0.7 * meal_stats['nutrition_score'] + 0.3 * meal_stats['frequency_score']
        
        # Limiter les ratings entre 1 et 5
        meal_stats['rating'] = meal_stats['rating'].clip(1, 5)
        
        return meal_stats[['user_id', 'meal_id', 'rating']]

    def load_data(self):
        """Charge toutes les données nécessaires depuis S3"""
        try:
            # Charger les données filtrées
            filtered_data = self.load_dataframe_from_s3(
                'transform/folder_2_filter_data/combined_meal_data_filtered.xlsx'
            )
            
            if filtered_data is not None:
                print("Données chargées, calcul des ratings implicites...")
                
                # Convertir les colonnes en numérique
                numeric_columns = ['total_calories', 'total_protein', 'total_lipids', 'total_carbs']
                for col in numeric_columns:
                    filtered_data[col] = pd.to_numeric(filtered_data[col], errors='coerce')
                
                # Calculer les ratings implicites
                ratings_data = self.calculate_implicit_ratings(filtered_data)
                
                # Créer la matrice utilisateur-repas
                self.ratings_matrix = pd.pivot_table(
                    ratings_data,
                    values='rating',
                    index='user_id',
                    columns='meal_id',
                    fill_value=0
                )
                
                # Stocker les caractéristiques des repas
                self.meal_features = filtered_data.groupby('meal_id').agg({
                    'total_calories': 'mean',
                    'total_protein': 'mean',
                    'total_lipids': 'mean',
                    'total_carbs': 'mean',
                    'Aliment': lambda x: ', '.join(x.unique())  # Liste des aliments dans le repas
                }).reset_index()
                
                print("Données chargées avec succès")
                print(f"Dimensions de la matrice de ratings: {self.ratings_matrix.shape}")
                print("\nAperçu des ratings calculés:")
                print(ratings_data.head())
                print("\nDistribution des ratings:")
                print(ratings_data['rating'].describe())
            else:
                print("Erreur: Impossible de charger les données filtrées")
                
        except Exception as e:
            print(f"Erreur lors du chargement des données: {str(e)}")
            import traceback
            print(traceback.format_exc())
    
    def save_recommendations(self, user_id: int, recommendations: List[Tuple[str, float]]):
        """
        Sauvegarde les recommandations dans S3
        
        Args:
            user_id: ID de l'utilisateur
            recommendations: Liste de tuples (type_aliment, score)
        """
        try:
            # Créer un DataFrame avec les recommandations
            recommendations_df = pd.DataFrame(recommendations, columns=['type_aliment', 'score'])
            recommendations_df['user_id'] = user_id
            recommendations_df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Sauvegarder dans S3
            with tempfile.NamedTemporaryFile(suffix='.xlsx') as tmp:
                recommendations_df.to_excel(tmp.name, index=False)
                self.s3_manager.s3_client.upload_file(
                    tmp.name,
                    self.s3_manager.bucket_name,
                    f'recommendations/user_{user_id}_recommendations.xlsx'
                )
                
        except Exception as e:
            print(f"Erreur lors de la sauvegarde des recommandations : {str(e)}")

if __name__ == "__main__":
    # Test du chargement des données
    recommender = BaseRecommender()
    recommender.load_data()
