"""
Script d'entraînement pour le recommender basé sur le contenu.
"""
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import sys

# Ajouter le chemin racine au PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from AI.recommender.content_based.food_recommender import ContentBasedRecommender
from AI.recommender.content_based.data_loader import ContentBasedDataLoader
from aws_s3.connect_s3 import S3Manager

def clean_numeric_value(value):
    """
    Nettoie une valeur numérique en gérant les cas spéciaux.
    """
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    # Si c'est une chaîne, la nettoyer
    try:
        # Remplacer la virgule par un point
        value = str(value).replace(',', '.')
        # Garder uniquement le dernier point
        parts = value.split('.')
        if len(parts) > 2:
            value = parts[0] + '.' + ''.join(parts[1:])
        return float(value)
    except:
        return 0.0

def train_and_save_model():
    """
    Entraîne et sauvegarde le modèle de recommandation basé sur le contenu.
    """
    print("Chargement des données...")
    data_loader = ContentBasedDataLoader()
    s3_manager = S3Manager()
    
    # Charger les données
    food_features, user_preferences = data_loader.load_training_data()
    
    if food_features is None or user_preferences is None:
        print("Erreur lors du chargement des données")
        return
        
    print("Préparation des données...")
    
    # Afficher les colonnes pour debug
    print("Colonnes dans food_features:", food_features.columns.tolist())
    
    # Nettoyer les valeurs numériques
    numeric_columns = ['calories', 'lipides', 'glucides', 'proteines', 'fibres', 'sucres', 'sodium']
    for col in numeric_columns:
        food_features[col] = food_features[col].apply(clean_numeric_value)
    
    # Afficher les statistiques après nettoyage
    print("\nStatistiques des features après nettoyage:")
    print(food_features[numeric_columns].describe())
    
    # Vérifier les correspondances d'IDs
    print("\nVérification des IDs:")
    print(f"Nombre d'aliments dans food_features: {len(food_features)}")
    print(f"Nombre d'aliments uniques dans user_preferences: {len(user_preferences['id'].unique())}")
    print(f"Nombre d'aliments communs: {len(set(food_features['id']).intersection(set(user_preferences['id'])))}")
    
    print("\nEntraînement du modèle...")
    recommender = ContentBasedRecommender()
    
    # Diviser les données pour le test
    print("\nPréparation des données de test...")
    test_size = 0.2
    
    # S'assurer que chaque utilisateur a des données dans le train et le test
    train_preferences = pd.DataFrame()
    test_preferences = pd.DataFrame()
    
    for user_id in user_preferences['user_id'].unique():
        user_data = user_preferences[user_preferences['user_id'] == user_id]
        n_test = max(1, int(len(user_data) * test_size))  # Au moins 1 item dans le test
        
        # Mélanger aléatoirement les données de l'utilisateur
        user_data = user_data.sample(frac=1, random_state=42)
        
        # Diviser en train et test
        user_test = user_data.iloc[:n_test]
        user_train = user_data.iloc[n_test:]
        
        test_preferences = pd.concat([test_preferences, user_test])
        train_preferences = pd.concat([train_preferences, user_train])
    
    print(f"Données d'entraînement : {len(train_preferences)} interactions")
    print(f"Données de test : {len(test_preferences)} interactions")
    
    # Entraîner le modèle sur les données d'entraînement
    recommender.fit(food_features, train_preferences)
    
    print("\nCalcul des statistiques...")
    # Calculer les statistiques sur les données de test
    stats = recommender.calculate_model_statistics(test_preferences, train_preferences)  # Ajout des données d'entraînement
    
    if stats:
        # Préparer les statistiques pour la sauvegarde
        model_stats = {
            'model_metrics': {
                'content_based': {
                    'mae': float(stats['mae']),
                    'rmse': float(stats['rmse']),
                    'coverage': float(stats['coverage'])
                }
            },
            'general_statistics': {
                'total_users': int(len(user_preferences['user_id'].unique())),
                'total_items': int(len(food_features)),
                'total_interactions': int(len(user_preferences)),
                'timestamp': datetime.now().isoformat()
            }
        }
        
        # Sauvegarder les statistiques
        print("\nSauvegarde des statistiques dans S3...")
        stats_json = json.dumps(model_stats, indent=2)
        try:
            # Créer le dossier results s'il n'existe pas
            s3_manager.s3_client.put_object(
                Bucket=s3_manager.bucket_name,
                Key='AI/recommender/content_based/results/',
                Body=''
            )
            
            # Sauvegarder les statistiques
            s3_manager.s3_client.put_object(
                Bucket=s3_manager.bucket_name,
                Key='AI/recommender/content_based/results/stats.json',
                Body=stats_json
            )
            print("✓ Statistiques sauvegardées avec succès")
            print("\nContenu des statistiques :")
            print(json.dumps(model_stats, indent=2))
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde des statistiques : {str(e)}")
            print("Contenu qui n'a pas pu être sauvegardé :")
            print(stats_json)
    
    print("\nStatistiques du modèle :")
    print(f"MAE: {stats['mae']:.3f}")
    print(f"RMSE: {stats['rmse']:.3f}")
    print(f"Couverture: {stats['coverage']:.1f}%")
    
    print("\nGénération des recommandations d'exemple...")
    # Générer des recommandations pour plus d'utilisateurs
    example_users = user_preferences['user_id'].unique()[:20]  # Augmenté de 5 à 20 utilisateurs
    recommendations = {}
    
    for user_id in example_users:
        recs = recommender.recommend(user_id, user_preferences)
        if not recs.empty:
            recommendations[str(user_id)] = recs.to_dict('records')
    
    # Sauvegarder les recommandations
    print("\nSauvegarde des recommandations dans S3...")
    recommendations_json = json.dumps(recommendations, indent=2)
    try:
        s3_manager.s3_client.put_object(
            Bucket=s3_manager.bucket_name,
            Key='AI/recommender/content_based/results/recommendations.json',
            Body=recommendations_json
        )
        print("✓ Recommandations sauvegardées avec succès")
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des recommandations : {str(e)}")
    
    print("Sauvegarde du modèle...")
    # Sauvegarder le modèle localement
    model_path = os.path.join(
        os.path.dirname(__file__), 
        '../models/content_based_model.joblib'
    )
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    recommender.save_model(model_path)
    
    # Uploader le modèle sur S3
    with open(model_path, 'rb') as f:
        s3_manager.s3_client.put_object(
            Bucket=s3_manager.bucket_name,
            Key='AI/recommender/content_based/models/content_based_model.joblib',
            Body=f
        )
    
    print("Entraînement terminé avec succès!")

if __name__ == "__main__":
    train_and_save_model()
