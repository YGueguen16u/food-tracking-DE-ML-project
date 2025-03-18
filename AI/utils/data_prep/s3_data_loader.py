"""
Module pour charger les données d'entraînement depuis S3.
"""
import sys
import os
import pandas as pd
from typing import Dict, Optional

# Ajouter le chemin pour importer les modules S3
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../aws_s3'))
from connect_s3 import create_s3_client

class S3DataLoader:
    """Gère le chargement des données depuis S3 pour les modèles d'IA."""
    
    def __init__(self, bucket_name: str):
        """
        Initialise la connexion S3.
        
        Args:
            bucket_name: Nom du bucket S3
        """
        self.s3_client = create_s3_client()
        self.bucket = bucket_name
        
    def _load_dataframe_from_s3(self, key: str) -> pd.DataFrame:
        """
        Charge un fichier CSV/Excel depuis S3 en DataFrame.
        
        Args:
            key: Chemin du fichier dans le bucket
            
        Returns:
            pd.DataFrame: Données chargées
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            if key.endswith('.csv'):
                return pd.read_csv(obj['Body'])
            elif key.endswith('.xlsx'):
                return pd.read_excel(obj['Body'])
            else:
                raise ValueError(f"Format de fichier non supporté: {key}")
        except Exception as e:
            print(f"Erreur lors du chargement de {key}: {str(e)}")
            raise
            
    def load_training_data(
        self,
        transformed_data_prefix: str,
        user_table_key: str,
        food_table_key: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Charge toutes les données nécessaires pour l'entraînement.
        
        Args:
            transformed_data_prefix: Préfixe des données transformées
            user_table_key: Chemin de la table utilisateurs
            food_table_key: Chemin de la table aliments
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire des DataFrames chargés
        """
        data = {}
        
        # Charger les données transformées
        transform_files = {
            'filtered': f"{transformed_data_prefix}/filtered_data.csv",
            'grouped': f"{transformed_data_prefix}/grouped_data.csv",
            'daily_window': f"{transformed_data_prefix}/window_daily.csv",
            'user_window': f"{transformed_data_prefix}/window_user.csv",
            'food_window': f"{transformed_data_prefix}/window_food_type.csv",
            'daily_change': f"{transformed_data_prefix}/change_daily.csv",
            'user_change': f"{transformed_data_prefix}/change_user.csv",
            'food_change': f"{transformed_data_prefix}/change_food_type.csv"
        }
        
        for name, key in transform_files.items():
            try:
                data[name] = self._load_dataframe_from_s3(key)
            except Exception as e:
                print(f"Attention: Impossible de charger {name}: {str(e)}")
                data[name] = None
        
        # Charger les tables de référence
        try:
            data['users'] = self._load_dataframe_from_s3(user_table_key)
            # Exclure la colonne 'profil' pour les prédictions
            if 'profil' in data['users'].columns:
                data['users'] = data['users'].drop('profil', axis=1)
        except Exception as e:
            print(f"Erreur critique: Impossible de charger la table utilisateurs: {str(e)}")
            raise
            
        try:
            data['foods'] = self._load_dataframe_from_s3(food_table_key)
        except Exception as e:
            print(f"Erreur critique: Impossible de charger la table aliments: {str(e)}")
            raise
            
        return data
        
    def get_latest_model_version(self, model_prefix: str) -> Optional[str]:
        """
        Récupère la dernière version d'un modèle entraîné.
        
        Args:
            model_prefix: Préfixe du modèle (ex: 'time_series/prophet')
            
        Returns:
            Optional[str]: Version la plus récente ou None si aucune
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"models/{model_prefix}"
            )
            if 'Contents' in response:
                versions = [obj['Key'] for obj in response['Contents']]
                return max(versions)  # Retourne la dernière version
            return None
        except Exception as e:
            print(f"Erreur lors de la recherche de versions: {str(e)}")
            return None
