import numpy as np
import pandas as pd
from typing import List, Tuple
import sys
import joblib
from AI.recommender.collaborative_filtering.base_recommender import BaseRecommender
import boto3
import json
from datetime import datetime
from .user_user_cf import UserUserCF
from .item_item_cf import ItemItemCF
import os

class HybridRecommender(BaseRecommender):
    """Système de recommandation hybride combinant les approches user-user et item-item"""
    
    def __init__(self, user_model, item_model, alpha=0.5):
        """
        Initialise le système de recommandation hybride
        
        Args:
            user_model: Modèle de filtrage collaboratif user-user
            item_model: Modèle de filtrage collaboratif item-item
            alpha: Poids relatif des deux modèles (0 = uniquement item-item, 1 = uniquement user-user)
        """
        super().__init__()
        self.user_model = user_model
        self.item_model = item_model
        self.alpha = alpha
        
    def fit(self, train_data):
        """
        Entraîne le modèle sur les données
        
        Args:
            train_data (pd.DataFrame): Données d'entraînement avec colonnes [user_id, Type, rating]
        """
        # Les modèles sont déjà entraînés individuellement
        pass
        
    def predict(self, user_id, k=5):
        """
        Génère des recommandations pour un utilisateur
        
        Args:
            user_id: ID de l'utilisateur
            k (int): Nombre de recommandations à générer
            
        Returns:
            list: Liste de tuples (type_aliment, score)
        """
        # Obtenir les recommandations des deux modèles
        user_recs = dict(self.user_model.predict(user_id, k=k))
        item_recs = dict(self.item_model.predict(user_id, k=k))
        
        # Combiner les scores
        combined_scores = {}
        all_types = set(user_recs.keys()) | set(item_recs.keys())
        
        for food_type in all_types:
            user_score = user_recs.get(food_type, 0)
            item_score = item_recs.get(food_type, 0)
            combined_scores[food_type] = self.alpha * user_score + (1 - self.alpha) * item_score
            
        # Retourner les k meilleures recommandations
        recommendations = [
            (food_type, score)
            for food_type, score in sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)[:k]
        ]
        
        return recommendations
        
    def evaluate(self, test_data):
        """
        Évalue le modèle sur les données de test
        
        Args:
            test_data (pd.DataFrame): Données de test
            
        Returns:
            dict: Métriques d'évaluation
        """
        predictions = []
        actuals = []
        
        for user_id in test_data['user_id'].unique():
            user_test = test_data[test_data['user_id'] == user_id]
            recs = self.predict(user_id)
            
            for _, row in user_test.iterrows():
                if row['Type'] in dict(recs):
                    predictions.append(dict(recs)[row['Type']])
                    actuals.append(row['rating'])
        
        if not predictions:
            return {'mae': float('inf'), 'rmse': float('inf'), 'coverage': 0.0}
            
        mae = np.mean(np.abs(np.array(predictions) - np.array(actuals)))
        rmse = np.sqrt(np.mean((np.array(predictions) - np.array(actuals)) ** 2))
        coverage = len(set(dict(recs).keys())) / len(self.user_model.ratings_matrix.columns)
        
        return {'mae': mae, 'rmse': rmse, 'coverage': coverage}
        
    def save(self, filepath: str) -> None:
        """
        Sauvegarde le modèle entraîné
        
        Args:
            filepath: Chemin où sauvegarder le modèle
        """
        model_data = {
            'user_model': self.user_model,
            'item_model': self.item_model,
            'alpha': self.alpha
        }
        joblib.dump(model_data, filepath)
