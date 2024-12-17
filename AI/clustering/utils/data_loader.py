import pandas as pd
import io
from aws_s3.connect_s3 import S3Manager

class ClusteringDataLoader:
    """Charge les données depuis S3 pour le clustering"""
    
    def __init__(self):
        """Initialise la connexion S3"""
        self.s3_manager = S3Manager()
        
    def load_dataframe_from_s3(self, s3_key):
        """Charge un DataFrame depuis S3"""
        try:
            response = self.s3_manager.s3_client.get_object(
                Bucket=self.s3_manager.bucket_name, 
                Key=s3_key
            )
            return pd.read_excel(io.BytesIO(response['Body'].read()))
        except Exception as e:
            print(f"Erreur lors du chargement de {s3_key}: {str(e)}")
            return None
    
    def load_clustering_data(self):
        """Charge et prépare les données pour le clustering"""
        # Données des proportions alimentaires par utilisateur
        food_proportions = self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/type_food/user_food_proportion_duckdb.xlsx'
        )
        
        # Données des moyennes quotidiennes
        daily_stats = self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/daily/daily_window_function_duckdb.xlsx'
        )
        
        return food_proportions, daily_stats
