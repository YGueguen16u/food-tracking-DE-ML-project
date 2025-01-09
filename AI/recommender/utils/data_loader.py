import pandas as pd
import io
from aws_s3.connect_s3 import S3Manager

class DataLoader:
    """Classe pour charger les données d'entraînement"""
    
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
        
    def load_training_data(self):
        """
        Charge les données pour l'entraînement
        
        Returns:
            tuple: (données d'entraînement, données de test)
        """
        try:
            # Charger les données combinées filtrées
            combined_data = self.load_dataframe_from_s3('transform/folder_2_filter_data/combined_meal_data_filtered.xlsx')
            if combined_data is None:
                return None, None
                
            # Charger les données de type d'aliments
            food_types = self.load_dataframe_from_s3('reference_data/food/food_processed.xlsx')
            if food_types is None:
                return None, None
                
            return combined_data, food_types
            
        except Exception as e:
            print(f"Erreur lors du chargement des données : {str(e)}")
            return None, None
