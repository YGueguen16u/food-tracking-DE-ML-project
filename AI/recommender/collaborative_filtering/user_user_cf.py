import os
import sys
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Tuple
import joblib

class UserUserCF:
    """Système de recommandation basé sur la similarité entre utilisateurs"""
    
    def __init__(self):
        """Initialise le système de recommandation"""
        self.user_similarity = None
        self.ratings_matrix = None
        
    def fit(self, train_data):
        """
        Entraîne le modèle sur les données
        
        Args:
            train_data (pd.DataFrame): Données d'entraînement avec colonnes [user_id, Type, rating]
        """
        # Créer la matrice utilisateur-type d'aliment
        self.ratings_matrix = train_data.pivot(
            index='user_id',
            columns='Type',
            values='rating'
        ).fillna(0)
        
        # Calculer les similarités entre utilisateurs
        self.user_similarity = pd.DataFrame(
            cosine_similarity(self.ratings_matrix),
            index=self.ratings_matrix.index,
            columns=self.ratings_matrix.index
        )
    
    def predict(self, user_id, k=5):
        """
        Génère des recommandations pour un utilisateur
        
        Args:
            user_id: ID de l'utilisateur
            k (int): Nombre de recommandations à générer
            
        Returns:
            list: Liste de tuples (type_aliment, score)
        """
        if user_id not in self.ratings_matrix.index:
            print(f"Utilisateur {user_id} non trouvé dans les données d'entraînement")
            return []
            
        # Obtenir les k utilisateurs les plus similaires
        similar_users = self.user_similarity[user_id].sort_values(ascending=False)[1:k+1]
        
        # Calculer les scores prédits pour chaque type d'aliment
        user_ratings = self.ratings_matrix.loc[similar_users.index]
        weights = similar_users.values.reshape(-1, 1)
        weighted_ratings = user_ratings * weights
        predicted_ratings = weighted_ratings.mean(axis=0)
        
        # Filtrer les types d'aliments déjà consommés
        user_consumed = self.ratings_matrix.loc[user_id] > 0
        predicted_ratings = predicted_ratings[~user_consumed]
        
        # Retourner les k meilleures recommandations
        recommendations = [
            (food_type, score) 
            for food_type, score in predicted_ratings.nlargest(k).items()
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
        coverage = len(set(dict(recs).keys())) / len(self.ratings_matrix.columns)
        
        return {'mae': mae, 'rmse': rmse, 'coverage': coverage}

    def save(self, filepath: str) -> None:
        """
        Sauvegarde le modèle entraîné
        
        Args:
            filepath (str): Chemin où sauvegarder le modèle
        """
        model_data = {
            'ratings_matrix': self.ratings_matrix,
            'user_similarity': self.user_similarity
        }
        joblib.dump(model_data, filepath)
