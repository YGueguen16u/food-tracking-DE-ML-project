"""
Recommandeur basé sur le contenu pour les aliments
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
import joblib
import os

class ContentBasedRecommender:
    """
    Système de recommandation basé sur le contenu pour les aliments.
    Utilise les caractéristiques nutritionnelles et les types d'aliments
    pour générer des recommandations personnalisées.
    """
    
    def __init__(self):
        """Initialise le recommender"""
        self.scaler = StandardScaler()
        self.food_features = None
        self.food_vectors = None
        self.similarity_matrix = None
        self.food_info = None
        
    def fit(self, food_features, user_preferences):
        """
        Entraîne le modèle avec les caractéristiques des aliments
        et les préférences utilisateurs.
        
        Args:
            food_features (pd.DataFrame): DataFrame avec les caractéristiques des aliments
            user_preferences (pd.DataFrame): DataFrame avec les préférences utilisateurs
        """
        # Sauvegarder les informations des aliments
        self.food_info = food_features[['id', 'Nom', 'Type']].copy()
        self.food_info.set_index('id', inplace=True)
        
        # Extraire les caractéristiques numériques pour la normalisation
        numeric_features = food_features[[
            'calories', 'lipides', 'glucides', 'proteines', 
            'fibres', 'sucres', 'sodium'
        ]].copy()
        
        # Normaliser les caractéristiques numériques
        normalized_features = pd.DataFrame(
            self.scaler.fit_transform(numeric_features),
            columns=numeric_features.columns,
            index=food_features['id']
        )
        
        # Encoder le type d'aliment
        type_dummies = pd.get_dummies(food_features['Type'], prefix='type')
        type_dummies.index = food_features['id']
        
        # Combiner les features
        self.food_vectors = pd.concat([normalized_features, type_dummies], axis=1)
        
        # Calculer la matrice de similarité
        self.similarity_matrix = cosine_similarity(self.food_vectors)
        
        return self
        
    def get_user_profile(self, user_id, user_preferences):
        """
        Calcule le profil d'un utilisateur basé sur ses préférences.
        
        Args:
            user_id: ID de l'utilisateur
            user_preferences (pd.DataFrame): DataFrame des préférences
            
        Returns:
            np.array: Vecteur du profil utilisateur
        """
        # Obtenir les aliments notés par l'utilisateur
        user_foods = user_preferences[user_preferences['user_id'] == user_id]
        
        if user_foods.empty:
            return None
            
        # Obtenir les caractéristiques des aliments aimés
        food_ids = user_foods['id'].values
        liked_foods_features = self.food_vectors[self.food_vectors.index.isin(food_ids)]
        
        if liked_foods_features.empty:
            return None
            
        # Calculer le profil comme une moyenne pondérée
        weights = user_foods.set_index('id')['rating']
        weights = weights[liked_foods_features.index]
        user_profile = np.average(liked_foods_features, axis=0, weights=weights)
        
        return user_profile
        
    def recommend(self, user_id, user_preferences, n_recommendations=5):
        """
        Génère des recommandations pour un utilisateur.
        
        Args:
            user_id: ID de l'utilisateur
            user_preferences (pd.DataFrame): DataFrame des préférences
            n_recommendations (int): Nombre de recommandations à générer
            
        Returns:
            pd.DataFrame: DataFrame avec les recommandations
        """
        # Obtenir le profil utilisateur
        user_profile = self.get_user_profile(user_id, user_preferences)
        
        if user_profile is None:
            return pd.DataFrame()
            
        # Calculer la similarité avec tous les aliments
        similarities = cosine_similarity([user_profile], self.food_vectors)[0]
        
        # Obtenir les aliments déjà notés par l'utilisateur
        user_foods = user_preferences[user_preferences['user_id'] == user_id]['id'].values
        
        # Créer un DataFrame avec les similarités
        sim_df = pd.DataFrame({
            'id': self.food_vectors.index,
            'similarity': similarities
        })
        
        # Exclure les aliments déjà notés
        sim_df = sim_df[~sim_df['id'].isin(user_foods)]
        
        # Trier par similarité et prendre les n meilleurs
        recommendations = sim_df.nlargest(n_recommendations, 'similarity')
        
        # Ajouter les informations des aliments
        recommendations = recommendations.join(
            self.food_info,
            on='id'
        )
        
        return recommendations
        
    def calculate_model_statistics(self, test_preferences, train_preferences):
        """
        Calcule les statistiques de performance du modèle.
        
        Args:
            test_preferences (pd.DataFrame): Données de test
            train_preferences (pd.DataFrame): Données d'entraînement
            
        Returns:
            dict: Dictionnaire avec les métriques
        """
        if test_preferences.empty:
            return None
            
        mae_sum = 0
        rmse_sum = 0
        coverage_items = set()
        total_predictions = 0
        
        print("\nCalcul des statistiques du modèle...")
        # Pour chaque utilisateur dans le test set
        for user_id in test_preferences['user_id'].unique():
            print(f"\nTraitement de l'utilisateur {user_id}")
            
            # Obtenir les vraies préférences
            user_true_ratings = test_preferences[
                test_preferences['user_id'] == user_id
            ].set_index('id')['rating']
            
            print(f"Nombre de ratings dans le test : {len(user_true_ratings)}")
            
            # Obtenir le profil utilisateur à partir des données d'entraînement
            user_train_data = train_preferences[train_preferences['user_id'] == user_id]
            if not user_train_data.empty:
                # Obtenir les caractéristiques des aliments aimés dans l'ensemble d'entraînement
                food_ids = user_train_data['id'].values
                liked_foods_features = self.food_vectors[self.food_vectors.index.isin(food_ids)]
                
                if not liked_foods_features.empty:
                    # Calculer le profil comme une moyenne pondérée
                    weights = user_train_data.set_index('id')['rating']
                    weights = weights[liked_foods_features.index]
                    user_profile = np.average(liked_foods_features, axis=0, weights=weights)
                    
                    # Calculer la similarité avec tous les aliments
                    similarities = cosine_similarity([user_profile], self.food_vectors)[0]
                    
                    # Créer un DataFrame avec les similarités pour tous les aliments
                    recs = pd.DataFrame({
                        'id': self.food_vectors.index,
                        'similarity': similarities
                    })
                    
                    # Ajouter les items recommandés à la couverture
                    coverage_items.update(recs['id'].values)
                    
                    # Calculer les erreurs pour les items qui sont dans le test set
                    test_items = recs[recs['id'].isin(user_true_ratings.index)]
                    
                    for _, rec in test_items.iterrows():
                        true_rating = user_true_ratings[rec['id']]
                        pred_rating = 5 * rec['similarity']  # Normaliser à l'échelle 0-5
                        
                        mae_sum += abs(true_rating - pred_rating)
                        rmse_sum += (true_rating - pred_rating) ** 2
                        total_predictions += 1
                    
                    print(f"Nombre de prédictions pour cet utilisateur : {len(test_items)}")
                else:
                    print("Aucun aliment trouvé dans les données d'entraînement")
            else:
                print("Aucune donnée d'entraînement pour cet utilisateur")
        
        print(f"\nNombre total de prédictions : {total_predictions}")
        print(f"Nombre d'items dans la couverture : {len(coverage_items)}")
        
        # Calculer les métriques finales
        if total_predictions > 0:
            mae = mae_sum / total_predictions
            rmse = np.sqrt(rmse_sum / total_predictions)
            coverage = len(coverage_items) / len(self.food_info) * 100
            
            print("\nMétriques finales :")
            print(f"MAE : {mae:.3f}")
            print(f"RMSE : {rmse:.3f}")
            print(f"Couverture : {coverage:.1f}%")
            
            return {
                'mae': mae,
                'rmse': rmse,
                'coverage': coverage
            }
        
        print("\nAucune prédiction n'a pu être générée")
        return None
        
    def save_model(self, path):
        """
        Sauvegarde le modèle sur disque.
        
        Args:
            path (str): Chemin où sauvegarder le modèle
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump(self, path)
        
    @classmethod
    def load_model(cls, path):
        """
        Charge un modèle depuis le disque.
        
        Args:
            path (str): Chemin du modèle
            
        Returns:
            ContentBasedRecommender: Instance du modèle chargé
        """
        return joblib.load(path)
