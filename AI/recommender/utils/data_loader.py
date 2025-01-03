import pandas as pd
import io
from aws_s3.connect_s3 import S3Manager

class DataLoader:
    """Charge les données depuis S3 pour le système de recommandation"""
    
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
            response = self.s3_manager.s3_client.get_object(
                Bucket=self.s3_manager.bucket_name, 
                Key=s3_key
            )
            return pd.read_excel(io.BytesIO(response['Body'].read()))
        except Exception as e:
            print(f"Erreur lors du chargement de {s3_key}: {str(e)}")
            return None
    
    def load_ratings_data(self):
        """
        Charge les données de ratings depuis S3
        
        Returns:
            pd.DataFrame: DataFrame des ratings utilisateur-repas
        """
        return self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/ratings/user_meal_ratings.xlsx'
        )
    
    def load_meal_features(self):
        """
        Charge les caractéristiques des repas depuis S3
        
        Returns:
            pd.DataFrame: DataFrame des caractéristiques des repas
        """
        return self.load_dataframe_from_s3(
            'transform/folder_2_filter_data/combined_meal_data_filtered.xlsx'
        )
    
    def load_user_preferences(self):
        """
        Charge les préférences utilisateur depuis S3
        
        Returns:
            pd.DataFrame: DataFrame des préférences utilisateur
        """
        return self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/preferences/user_preferences.xlsx'
        )
    
    def save_recommendations_to_s3(self, user_id, recommendations, timestamp):
        """
        Sauvegarde les recommandations dans S3
        
        Args:
            user_id (int): ID de l'utilisateur
            recommendations (list): Liste des recommandations
            timestamp (str): Horodatage des recommandations
        """
        try:
            # Convertir les recommandations en DataFrame
            recommendations_df = pd.DataFrame(recommendations)
            
            # Créer un buffer en mémoire
            buffer = io.BytesIO()
            recommendations_df.to_excel(buffer, index=False)
            buffer.seek(0)
            
            # Sauvegarder dans S3
            s3_key = f'recommendations/user_{user_id}/{timestamp}_recommendations.xlsx'
            self.s3_manager.s3_client.put_object(
                Bucket=self.s3_manager.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            print(f"Recommandations sauvegardées dans {s3_key}")
            
        except Exception as e:
            print(f"Erreur lors de la sauvegarde des recommandations: {str(e)}")
