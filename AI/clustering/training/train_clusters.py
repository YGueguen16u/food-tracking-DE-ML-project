import sys
import os
import json

# Ajouter les répertoires nécessaires au PYTHONPATH
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.extend([current_dir, project_root])

from utils.data_loader import ClusteringDataLoader
from models.kmeans_clustering import UserClusteringModel
from aws_s3.connect_s3 import S3Manager
import pandas as pd
from datetime import datetime

def train_and_save_clusters():
    """Entraîne le modèle de clustering et sauvegarde les résultats dans S3"""
    
    # Initialiser S3Manager
    s3_manager = S3Manager()
    
    # Charger les données
    data_loader = ClusteringDataLoader()
    user_data = data_loader.load_dataframe_from_s3(
        'transform/folder_2_filter_data/combined_meal_data_filtered.xlsx'
    )
    
    # Convertir les colonnes en nombres
    numeric_columns = ['total_calories', 'total_lipids', 'total_carbs', 'total_protein', 'quantity']
    for col in numeric_columns:
        user_data[col] = pd.to_numeric(user_data[col], errors='coerce')
    
    # Créer et entraîner le modèle
    model = UserClusteringModel(n_clusters=6)  # 6 clusters comme dans vos données
    results, cluster_analysis = model.fit(user_data)
    
    # Sauvegarder les résultats dans S3
    # 1. Résultats par utilisateur
    results_key = "AI/clustering/results/user_clusters.xlsx"
    with pd.ExcelWriter('/tmp/temp_results.xlsx') as writer:
        results.to_excel(writer, index=False)
    s3_manager.upload_with_overwrite('/tmp/temp_results.xlsx', results_key)
    os.remove('/tmp/temp_results.xlsx')
    
    # 2. Analyse des clusters
    analysis_key = "AI/clustering/results/cluster_analysis.json"
    with open('/tmp/temp_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(cluster_analysis, f, ensure_ascii=False, indent=4)
    s3_manager.upload_with_overwrite('/tmp/temp_analysis.json', analysis_key)
    os.remove('/tmp/temp_analysis.json')
    
    # 3. Sauvegarder le modèle entraîné
    model_data = {
        'n_clusters': model.n_clusters,
        'cluster_centers': model.kmeans.cluster_centers_.tolist(),
        'scaler_mean': model.scaler.mean_.tolist(),
        'scaler_scale': model.scaler.scale_.tolist()
    }
    model_key = "AI/clustering/models/kmeans_model.json"
    with open('/tmp/temp_model.json', 'w') as f:
        json.dump(model_data, f)
    s3_manager.upload_with_overwrite('/tmp/temp_model.json', model_key)
    os.remove('/tmp/temp_model.json')
    
    print(f"\nRésultats sauvegardés dans S3:")
    print(f"- Résultats utilisateurs: {results_key}")
    print(f"- Analyse des clusters: {analysis_key}")
    print(f"- Modèle: {model_key}")
    
    # Afficher un résumé
    print("\nAnalyse des clusters :")
    for cluster_id, stats in cluster_analysis.items():
        print(f"\n{cluster_id.upper()} :")
        print(f"Nombre d'utilisateurs : {stats['nombre_utilisateurs']}")
        print(f"Moyenne repas par jour : {stats['repas_par_jour']:.2f}")
        print(f"Heures principales des repas : {stats['heures_principales']}")
        print("Aliments les plus fréquents :")
        for aliment, count in list(stats['aliments_frequents'].items())[:3]:
            print(f"  - {aliment}: {count}")
        print("Moyennes des nutriments :")
        for nutrient, value in stats['moyennes_nutriments'].items():
            print(f"  - {nutrient}: {value:.2f}")

if __name__ == "__main__":
    train_and_save_clusters()
