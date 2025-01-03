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

class UserUserCF(BaseRecommender):
    """Système de recommandation basé sur le filtrage collaboratif utilisateur-utilisateur"""
    
    def __init__(self):
        """Initialise le système de recommandation"""
        super().__init__()
        self.user_similarity_matrix = None
        self.user_means = None
        
    def compute_user_similarity(self):
        """Calcule la matrice de similarité entre utilisateurs"""
        if self.ratings_matrix is None:
            print("Données non chargées. Appelez load_data() d'abord.")
            return
            
        n_users = len(self.ratings_matrix.index)
        user_ids = self.ratings_matrix.index
        
        # Calculer les moyennes par utilisateur
        self.user_means = self.ratings_matrix.mean(axis=1)
        
        # Normaliser les ratings
        normalized_matrix = self.ratings_matrix.sub(self.user_means, axis=0)
        
        # Initialiser la matrice de similarité
        similarity_matrix = np.zeros((n_users, n_users))
        
        # Calculer les similarités
        for i in range(n_users):
            for j in range(i+1, n_users):
                user1_vector = normalized_matrix.iloc[i].values
                user2_vector = normalized_matrix.iloc[j].values
                
                # Calculer similarité uniquement sur les items communs
                mask = (user1_vector != 0) & (user2_vector != 0)
                if mask.sum() > 0:
                    similarity = 1 - cosine(user1_vector[mask], user2_vector[mask])
                else:
                    similarity = 0
                    
                similarity_matrix[i, j] = similarity
                similarity_matrix[j, i] = similarity
        
        self.user_similarity_matrix = pd.DataFrame(
            similarity_matrix,
            index=user_ids,
            columns=user_ids
        )
        
    def get_similar_users(self, user_id: int, n: int = 5) -> List[Tuple[int, float]]:
        """
        Trouve les N utilisateurs les plus similaires
        
        Args:
            user_id: ID de l'utilisateur
            n: Nombre d'utilisateurs similaires à retourner
            
        Returns:
            Liste de tuples (user_id, score_similarité)
        """
        if self.user_similarity_matrix is None:
            print("Similarités non calculées. Appelez compute_user_similarity() d'abord.")
            return []
            
        if user_id not in self.user_similarity_matrix.index:
            return []
            
        similarities = self.user_similarity_matrix.loc[user_id]
        top_similar = similarities.nlargest(n+1)[1:n+1]  # Exclure l'utilisateur lui-même
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
        if user_id not in self.ratings_matrix.index:
            return self.user_means.mean()
            
        similar_users = self.get_similar_users(user_id)
        if not similar_users:
            return self.user_means[user_id]
            
        numerator = 0
        denominator = 0
        
        for sim_user_id, similarity in similar_users:
            if meal_id in self.ratings_matrix.columns:
                rating = self.ratings_matrix.loc[sim_user_id, meal_id]
                if rating != 0:  # Si l'utilisateur a noté le repas
                    numerator += similarity * (rating - self.user_means[sim_user_id])
                    denominator += abs(similarity)
        
        if denominator == 0:
            return self.user_means[user_id]
            
        predicted_rating = self.user_means[user_id] + (numerator / denominator)
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
    recommender = UserUserCF()
    recommender.load_data()
    recommender.compute_user_similarity()
    recommendations = recommender.get_recommendations(user_id=1)
    print("Recommandations:", recommendations)
