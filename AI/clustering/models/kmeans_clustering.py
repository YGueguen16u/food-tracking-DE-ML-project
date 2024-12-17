import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from typing import Tuple, Dict
import datetime

class UserClusteringModel:
    """Modèle de clustering pour regrouper les utilisateurs selon leurs habitudes alimentaires"""
    
    def __init__(self, n_clusters: int = 6):  # 6 clusters comme dans vos données
        self.n_clusters = n_clusters
        self.scaler = StandardScaler()
        self.kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        
    def prepare_features(self, user_data: pd.DataFrame) -> np.ndarray:
        """Prépare les features pour le clustering"""
        features_dict = {}
        
        # 1. Nombre de repas par jour par utilisateur
        meals_per_day = user_data.groupby(['user_id', 'date']).size().reset_index()
        meals_per_day_avg = meals_per_day.groupby('user_id')[0].mean()
        meals_per_day_avg.name = 'meals_per_day'  # Donner un nom explicite à la série
        features_dict['meals_per_day'] = meals_per_day_avg
        
        # 2. Distribution des heures de repas
        user_data['heure'] = pd.to_datetime(user_data['heure']).dt.hour
        hours_dist = pd.get_dummies(user_data[['user_id', 'heure']].set_index('user_id')).groupby('user_id').mean()
        # Renommer les colonnes pour qu'elles soient toutes des chaînes
        hours_dist.columns = [f'heure_{str(col)}' for col in hours_dist.columns]
        
        # 3. Types d'aliments consommés (proportions)
        food_types = pd.get_dummies(user_data[['user_id', 'Aliment']].set_index('user_id')).groupby('user_id').mean()
        food_types.columns = [f'aliment_{col}' for col in food_types.columns]
        
        # 4. Moyennes des nutriments
        nutrients = user_data.groupby('user_id').agg({
            'total_calories': 'mean',
            'total_lipids': 'mean',
            'total_carbs': 'mean',
            'total_protein': 'mean'
        })
        nutrients.columns = [f'nutriment_{col}' for col in nutrients.columns]
        
        # Combiner toutes les features
        features_df = pd.concat([
            features_dict['meals_per_day'],
            hours_dist,
            food_types,
            nutrients
        ], axis=1).fillna(0)
        
        # S'assurer que tous les noms de colonnes sont des chaînes
        features_df.columns = features_df.columns.astype(str)
        
        # Normaliser
        features = self.scaler.fit_transform(features_df)
        self.feature_names = features_df.columns
        
        return features, features_df.index
    
    def fit(self, user_data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Entraîne le modèle et retourne les résultats détaillés par utilisateur"""
        # Préparer les features
        features, user_ids = self.prepare_features(user_data)
        
        # Entraîner le modèle
        self.kmeans.fit(features)
        
        # Créer le DataFrame des résultats
        results = pd.DataFrame({
            'user_id': user_ids,
            'cluster': self.kmeans.labels_
        })
        
        # Fusionner avec les données originales pour l'analyse
        results = results.merge(user_data, on='user_id', how='left')
        
        # Analyser les clusters
        cluster_analysis = self._analyze_clusters(results)
        
        return results, cluster_analysis
    
    def _analyze_clusters(self, results: pd.DataFrame) -> Dict:
        """Analyse détaillée des clusters"""
        analysis = {}
        
        for cluster in range(self.n_clusters):
            cluster_data = results[results['cluster'] == cluster]
            
            # Statistiques générales
            stats = {
                'utilisateurs': cluster_data['user_id'].unique().tolist(),  # Liste des IDs des utilisateurs
                'nombre_utilisateurs': len(cluster_data['user_id'].unique()),
                'repas_par_jour': cluster_data.groupby(['user_id', 'date']).size().mean(),
                'heures_principales': cluster_data.groupby('heure').size().nlargest(3).index.tolist(),
                'aliments_frequents': (
                    cluster_data.groupby('Aliment').size()
                    .sort_values(ascending=False)
                    .head(5)
                    .to_dict()
                ),
                'moyennes_nutriments': {
                    'calories': cluster_data['total_calories'].mean(),
                    'lipides': cluster_data['total_lipids'].mean(),
                    'proteines': cluster_data['total_protein'].mean(),
                    'glucides': cluster_data['total_carbs'].mean()
                }
            }
            
            analysis[f'cluster_{cluster}'] = stats
        
        return analysis
