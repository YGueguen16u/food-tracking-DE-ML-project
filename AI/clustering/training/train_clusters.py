import sys
import os
import json

# Ajouter les répertoires nécessaires au PYTHONPATH
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.extend([current_dir, project_root])

from utils.data_loader import ClusteringDataLoader
from models.kmeans_clustering import UserClusteringModel
import pandas as pd
from datetime import datetime

def train_and_save_clusters():
    """Entraîne le modèle de clustering et sauvegarde les résultats"""
    
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
    
    # Créer le dossier results s'il n'existe pas
    os.makedirs('results', exist_ok=True)
    
    # Sauvegarder les résultats détaillés
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sauvegarder les résultats par utilisateur
    results.to_excel(f"results/user_clusters_{timestamp}.xlsx", index=False)
    
    # Sauvegarder l'analyse des clusters
    with open(f"results/cluster_analysis_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(cluster_analysis, f, ensure_ascii=False, indent=4)
    
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
