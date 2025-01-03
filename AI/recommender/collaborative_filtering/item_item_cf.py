import os
import sys

# Ajouter le chemin racine au PYTHONPATH
root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(root_path)

from AI.recommender.collaborative_filtering.base_recommender import BaseRecommender
import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from typing import List, Tuple

class ItemItemCF(BaseRecommender):
    """Système de recommandation basé sur le filtrage collaboratif item-item"""
    
    def __init__(self):
        """Initialise le système de recommandation"""
        super().__init__()
        self.item_similarity_matrix = None
        self.item_means = None
        
    def compute_item_similarity(self):
        """Calcule la matrice de similarité entre items"""
        if self.ratings_matrix is None:
            print("Données non chargées. Appelez load_data() d'abord.")
            return
            
        # Calculer les moyennes par item
        self.item_means = self.ratings_matrix.mean()
        
        # Normaliser les ratings
        normalized_matrix = self.ratings_matrix.sub(self.item_means, axis=1)
        
        # Calculer les similarités
        self.item_similarity_matrix = pd.DataFrame(
            index=self.ratings_matrix.columns,
            columns=self.ratings_matrix.columns,
            dtype=float
        )
        
        for i in self.ratings_matrix.columns:
            for j in self.ratings_matrix.columns:
                if i == j:
                    self.item_similarity_matrix.loc[i, j] = 1.0
                else:
                    # Obtenir les vecteurs de ratings
                    item1_vector = normalized_matrix[i].values
                    item2_vector = normalized_matrix[j].values
                    
                    # Calculer similarité uniquement sur les utilisateurs communs
                    mask = (item1_vector != 0) & (item2_vector != 0)
                    if mask.sum() > 0:
                        similarity = 1 - cosine(item1_vector[mask], item2_vector[mask])
                        self.item_similarity_matrix.loc[i, j] = similarity
                    else:
                        self.item_similarity_matrix.loc[i, j] = 0
        
    def get_similar_items(self, meal_id: str, n: int = 5) -> List[Tuple[str, float]]:
        """
        Trouve les N items les plus similaires
        
        Args:
            meal_id: ID du repas
            n: Nombre d'items similaires à retourner
            
        Returns:
            Liste de tuples (meal_id, score_similarité)
        """
        if self.item_similarity_matrix is None:
            print("Similarités non calculées. Appelez compute_item_similarity() d'abord.")
            return []
            
        if meal_id not in self.item_similarity_matrix.index:
            return []
            
        similarities = self.item_similarity_matrix.loc[meal_id]
        top_similar = similarities.nlargest(n+1)[1:n+1]  # Exclure l'item lui-même
        return list(zip(top_similar.index, top_similar.values))
        
    def predict_rating(self, user_id: int, meal_id: str) -> float:
        """
        Prédit la note pour un repas non noté
        
        Args:
            user_id: ID de l'utilisateur
            meal_id: ID du repas
            
        Returns:
            Note prédite
        """
        if meal_id not in self.item_similarity_matrix.index:
            return self.item_means.mean()
            
        similar_items = self.get_similar_items(meal_id)
        if not similar_items:
            return self.item_means[meal_id]
            
        numerator = 0
        denominator = 0
        
        user_ratings = self.ratings_matrix.loc[user_id]
        
        for sim_meal_id, similarity in similar_items:
            if sim_meal_id in user_ratings.index:
                rating = user_ratings[sim_meal_id]
                if rating != 0:  # Si l'utilisateur a noté le repas
                    numerator += similarity * (rating - self.item_means[sim_meal_id])
                    denominator += abs(similarity)
        
        if denominator == 0:
            return self.item_means[meal_id]
            
        predicted_rating = self.item_means[meal_id] + (numerator / denominator)
        return max(1, min(5, predicted_rating))  # Limiter entre 1 et 5
        
    def get_recommendations(self, user_id: int, n_recommendations: int = 5) -> List[Tuple[str, float]]:
        """
        Génère des recommandations pour un utilisateur
        
        Args:
            user_id: ID de l'utilisateur
            n_recommendations: Nombre de recommandations à générer
            
        Returns:
            Liste de tuples (meal_id, score_prédit)
        """
        if user_id not in self.ratings_matrix.index:
            return []
            
        # Trouver les repas non notés
        user_ratings = self.ratings_matrix.loc[user_id]
        unrated_meals = user_ratings[user_ratings == 0].index
        
        # Prédire les notes pour les repas non notés
        predictions = []
        for meal_id in unrated_meals:
            predicted_rating = self.predict_rating(user_id, meal_id)
            predictions.append((meal_id, predicted_rating))
        
        # Trier et retourner les top N recommandations
        recommendations = sorted(predictions, key=lambda x: x[1], reverse=True)[:n_recommendations]
        
        # Sauvegarder les recommandations
        self.save_recommendations(user_id, recommendations)
        
        return recommendations

if __name__ == "__main__":
    # Test du système de recommandation
    recommender = ItemItemCF()
    recommender.load_data()
    recommender.compute_item_similarity()
    recommendations = recommender.get_recommendations(user_id=1)
    print("Recommandations:", recommendations)
