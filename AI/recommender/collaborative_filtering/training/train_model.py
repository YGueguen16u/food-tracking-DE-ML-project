import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import json
import tempfile

# Ajouter le chemin racine au PYTHONPATH
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(root_dir)

from AI.recommender.collaborative_filtering.user_user_cf import UserUserCF
from AI.recommender.collaborative_filtering.item_item_cf import ItemItemCF
from AI.recommender.collaborative_filtering.hybrid_recommender import HybridRecommender
from AI.recommender.utils.data_loader import DataLoader
from aws_s3.connect_s3 import S3Manager

def prepare_data_for_training(combined_data, food_types):
    """
    Prépare les données pour l'entraînement en utilisant les types d'aliments
    """
    # Créer un mapping aliment_id -> type
    food_type_mapping = food_types.set_index('id')['Type'].to_dict()
    
    # Ajouter la colonne Type aux données combinées
    combined_data['Type'] = combined_data['aliment_id'].map(food_type_mapping)
    
    # Agréger par utilisateur et type d'aliment
    ratings_data = combined_data.groupby(['user_id', 'Type']).agg({
        'quantity': 'sum'
    }).reset_index()
    
    # Normaliser les quantités pour créer un score entre 1 et 5
    ratings_data['rating'] = 1 + (ratings_data['quantity'] - ratings_data['quantity'].min()) / \
                            (ratings_data['quantity'].max() - ratings_data['quantity'].min()) * 4
    
    # Garder uniquement les colonnes nécessaires
    ratings_data = ratings_data[['user_id', 'Type', 'rating']]
    
    # Diviser en ensembles d'entraînement et de test (80/20)
    train_data = ratings_data.sample(frac=0.8, random_state=42)
    test_data = ratings_data.drop(train_data.index)
    
    return train_data, test_data

def calculate_model_statistics(user_user_model, item_item_model, hybrid_model, test_data):
    """
    Calcule les statistiques détaillées des modèles de recommandation
    """
    stats = {
        'general_statistics': {
            'total_users': len(test_data['user_id'].unique()),
            'total_items': len(test_data['Type'].unique()),
            'total_interactions': len(test_data),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'model_metrics': {
            'user_user_cf': {
                'mae': user_user_model.evaluate(test_data)['mae'],
                'rmse': user_user_model.evaluate(test_data)['rmse'],
                'coverage': user_user_model.evaluate(test_data)['coverage']
            },
            'item_item_cf': {
                'mae': item_item_model.evaluate(test_data)['mae'],
                'rmse': item_item_model.evaluate(test_data)['rmse'],
                'coverage': item_item_model.evaluate(test_data)['coverage']
            },
            'hybrid': {
                'mae': hybrid_model.evaluate(test_data)['mae'],
                'rmse': hybrid_model.evaluate(test_data)['rmse'],
                'coverage': hybrid_model.evaluate(test_data)['coverage']
            }
        },
        'sample_recommendations': {}
    }
    
    # Ajouter des exemples de recommandations pour quelques utilisateurs
    sample_users = test_data['user_id'].unique()[:3]  # Prendre 3 utilisateurs au hasard
    
    for user_id in sample_users:
        user_recommendations = {
            'user_user_cf': [
                {'type': rec[0], 'score': float(rec[1])}
                for rec in user_user_model.predict(user_id, k=5)
            ],
            'item_item_cf': [
                {'type': rec[0], 'score': float(rec[1])}
                for rec in item_item_model.predict(user_id, k=5)
            ],
            'hybrid': [
                {'type': rec[0], 'score': float(rec[1])}
                for rec in hybrid_model.predict(user_id, k=5)
            ]
        }
        stats['sample_recommendations'][str(user_id)] = user_recommendations
    
    return stats

def train_and_save_models():
    """Entraîne et sauvegarde les modèles de recommandation"""
    
    print("Chargement des données...")
    loader = DataLoader()
    combined_data, food_types = loader.load_training_data()
    
    if combined_data is None or food_types is None:
        print("Erreur lors du chargement des données")
        return
        
    # Préparer les données pour l'entraînement
    train_data, test_data = prepare_data_for_training(combined_data, food_types)
    
    print("Entraînement des modèles...")
    
    # User-User CF
    print("1. Entraînement du modèle User-User CF...")
    user_user_model = UserUserCF()
    user_user_model.fit(train_data)
    
    # Item-Item CF
    print("2. Entraînement du modèle Item-Item CF...")
    item_item_model = ItemItemCF()
    item_item_model.fit(train_data)
    
    # Hybrid
    print("3. Entraînement du modèle Hybride...")
    hybrid_model = HybridRecommender(user_user_model, item_item_model)
    hybrid_model.fit(train_data)
    
    print("Évaluation des modèles...")
    stats = calculate_model_statistics(
        user_user_model,
        item_item_model,
        hybrid_model,
        test_data
    )
    
    print("Génération des recommandations...")
    recommendations = {'user_user_cf': {}, 'item_item_cf': {}, 'hybrid': {}}
    
    # Générer des recommandations pour tous les utilisateurs
    for user_id in test_data['user_id'].unique():
        recommendations['user_user_cf'][str(user_id)] = [
            {'type': rec[0], 'score': float(rec[1])}
            for rec in user_user_model.predict(user_id, k=10)
        ]
        recommendations['item_item_cf'][str(user_id)] = [
            {'type': rec[0], 'score': float(rec[1])}
            for rec in item_item_model.predict(user_id, k=10)
        ]
        recommendations['hybrid'][str(user_id)] = [
            {'type': rec[0], 'score': float(rec[1])}
            for rec in hybrid_model.predict(user_id, k=10)
        ]
    
    try:
        # 1. Sauvegarder les recommandations
        recommendations_key = "AI/recommender/collaborative_filtering/results/recommendations.json"
        temp_recommendations = os.path.join(tempfile.mkdtemp(), "recommendations.json")
        with open(temp_recommendations, 'w', encoding='utf-8') as f:
            json.dump(recommendations, f, indent=4, ensure_ascii=False)
        s3_manager = S3Manager()
        s3_manager.upload_with_overwrite(temp_recommendations, recommendations_key)
        
        # 2. Sauvegarder les statistiques
        stats_key = "AI/recommender/collaborative_filtering/results/stats.json"
        temp_stats = os.path.join(tempfile.mkdtemp(), "stats.json")
        with open(temp_stats, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=4, ensure_ascii=False)
        s3_manager.upload_with_overwrite(temp_stats, stats_key)
        
        # 3. Sauvegarder les modèles
        base_model_key = "AI/recommender/collaborative_filtering/models/"
        
        temp_user_model = os.path.join(tempfile.mkdtemp(), "user_user_model.joblib")
        user_user_model.save(temp_user_model)
        s3_manager.upload_with_overwrite(temp_user_model, base_model_key + "user_user_model.joblib")
        
        temp_item_model = os.path.join(tempfile.mkdtemp(), "item_item_model.joblib")
        item_item_model.save(temp_item_model)
        s3_manager.upload_with_overwrite(temp_item_model, base_model_key + "item_item_model.joblib")
        
        temp_hybrid_model = os.path.join(tempfile.mkdtemp(), "hybrid_model.joblib")
        hybrid_model.save(temp_hybrid_model)
        s3_manager.upload_with_overwrite(temp_hybrid_model, base_model_key + "hybrid_model.joblib")
        
        print("Modèles et résultats sauvegardés avec succès!")
        
    except Exception as e:
        print(f"Erreur lors de la sauvegarde : {str(e)}")
    finally:
        # Nettoyage : supprimer les fichiers temporaires
        import shutil
        shutil.rmtree(tempfile.mkdtemp(), ignore_errors=True)
    
    # Affichage de quelques statistiques clés
    print(f"\nStatistiques clés:")
    print(f"- Nombre total d'utilisateurs: {stats['general_statistics']['total_users']}")
    print(f"- Nombre total d'aliments: {stats['general_statistics']['total_items']}")
    print(f"- Performance du modèle hybride:")
    print(f"  * MAE: {stats['model_metrics']['hybrid']['mae']:.3f}")
    print(f"  * RMSE: {stats['model_metrics']['hybrid']['rmse']:.3f}")
    print(f"  * Couverture: {stats['model_metrics']['hybrid']['coverage']:.1f}%")

if __name__ == "__main__":
    train_and_save_models()
