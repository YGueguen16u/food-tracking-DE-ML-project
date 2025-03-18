import pandas as pd
import io
from aws_s3.connect_s3 import S3Manager

class DataLoader:
    """Charge les données depuis S3 pour la détection d'anomalies"""
    
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
        Charge les données d'entraînement depuis S3
        
        Returns:
            tuple: (données filtrées, moyennes quotidiennes, proportions par type)
        """
        # Charger les données filtrées
        filtered_data = self.load_dataframe_from_s3(
            'transform/folder_2_filter_data/combined_meal_data_filtered.xlsx'
        )
        
        # Charger les moyennes quotidiennes
        daily_stats = self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/daily/daily_window_function_duckdb.xlsx'
        )
        
        # Charger les proportions par type d'aliment
        food_proportions = self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/type_food/user_food_proportion_duckdb.xlsx'
        )
        
        return filtered_data, daily_stats, food_proportions
    
    def get_reference_data(self):
        """
        Charge les données de référence (aliments)
        
        Returns:
            pd.DataFrame: Données de référence des aliments
        """
        return self.load_dataframe_from_s3('reference_data/food/food_processed.xlsx')
