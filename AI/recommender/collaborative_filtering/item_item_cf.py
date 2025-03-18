import os
import sys
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from typing import List, Tuple
import joblib
from sklearn.metrics.pairwise import cosine_similarity

class BaseRecommender:
    pass

class ItemItemCF(BaseRecommender):
    """Système de recommandation basé sur la similarité entre types d'aliments"""
    
    def __init__(self):
        """Initialise le système de recommandation"""
        super().__init__()
        self.item_similarity = None
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
        
        # Calculer les similarités entre types d'aliments
        item_matrix = self.ratings_matrix.T
        self.item_similarity = pd.DataFrame(
            cosine_similarity(item_matrix),
            index=item_matrix.index,
            columns=item_matrix.index
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
            
        # Obtenir les notes de l'utilisateur
        user_ratings = self.ratings_matrix.loc[user_id]
        
        # Calculer les scores prédits pour chaque type d'aliment
        weighted_sum = pd.DataFrame(0, index=self.ratings_matrix.columns, columns=['score'])
        weight_sum = pd.DataFrame(0, index=self.ratings_matrix.columns, columns=['weight'])
        
        for item in user_ratings.index:
            if user_ratings[item] > 0:
                similarities = self.item_similarity[item]
                weighted_sum['score'] += similarities * user_ratings[item]
                weight_sum['weight'] += np.abs(similarities)
                
        # Éviter la division par zéro
        weight_sum.loc[weight_sum['weight'] == 0, 'weight'] = 1
        predicted_ratings = weighted_sum['score'] / weight_sum['weight']
        
        # Filtrer les types d'aliments déjà consommés
        user_consumed = user_ratings > 0
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
            filepath: Chemin où sauvegarder le modèle
        """
        model_data = {
            'ratings_matrix': self.ratings_matrix,
            'item_similarity': self.item_similarity
        }
        joblib.dump(model_data, filepath)
