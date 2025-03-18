import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# Ajouter le chemin racine au PYTHONPATH
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_dir)

from AI.anomaly_detection.models.anomaly_detector import MealAnomalyDetector, AnomalyAnalyzer
from AI.anomaly_detection.utils.data_loader import DataLoader

def evaluate_model(model_path, scaler_path):
    """
    Évalue le modèle sur un ensemble de test
    
    Args:
        model_path (str): Chemin vers le modèle sauvegardé
        scaler_path (str): Chemin vers le scaler sauvegardé
    """
    print("Chargement des données...")
    loader = DataLoader()
    filtered_data, daily_stats, food_proportions = loader.load_training_data()
    
    if filtered_data is None:
        print("Erreur lors du chargement des données")
        return
    
    print("Chargement du modèle...")
    detector = MealAnomalyDetector.load(model_path, scaler_path)
    analyzer = AnomalyAnalyzer()
    
    print("Prédiction des anomalies...")
    results = detector.predict(filtered_data)
    
    # Analyse globale
    anomaly_rate = (results['is_anomaly'].sum() / len(results)) * 100
    print(f"\nStatistiques globales:")
    print(f"- Nombre total de repas analysés: {len(results)}")
    print(f"- Nombre d'anomalies détectées: {results['is_anomaly'].sum()}")
    print(f"- Taux d'anomalies: {anomaly_rate:.2f}%")
    
    # Distribution des scores d'anomalie
    plt.figure(figsize=(10, 6))
    sns.histplot(data=results, x='anomaly_score', hue='is_anomaly', bins=50)
    plt.title('Distribution des scores d\'anomalie')
    plt.xlabel('Score d\'anomalie')
    plt.ylabel('Nombre de repas')
    
    # Sauvegarder le graphique
    plots_dir = os.path.join(os.path.dirname(__file__), 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    plt.savefig(os.path.join(plots_dir, 'anomaly_scores_distribution.png'))
    
    # Analyse temporelle
    results['hour'] = pd.to_datetime(results['heure']).dt.hour
    results['weekday'] = pd.to_datetime(results['date']).dt.dayofweek
    
    # Distribution des anomalies par heure
    plt.figure(figsize=(12, 6))
    anomaly_by_hour = results[results['is_anomaly']]['hour'].value_counts().sort_index()
    total_by_hour = results['hour'].value_counts().sort_index()
    anomaly_rate_by_hour = (anomaly_by_hour / total_by_hour * 100)
    
    sns.lineplot(x=anomaly_rate_by_hour.index, y=anomaly_rate_by_hour.values)
    plt.title('Taux d\'anomalies par heure')
    plt.xlabel('Heure')
    plt.ylabel('Taux d\'anomalies (%)')
    plt.savefig(os.path.join(plots_dir, 'anomaly_rate_by_hour.png'))
    
    # Analyse des raisons des anomalies
    print("\nAnalyse des anomalies détectées:")
    anomalies = results[results['is_anomaly']].copy()
    reasons_count = {}
    
    for _, anomaly in anomalies.iterrows():
        reasons = analyzer.analyze_anomaly(anomaly)
        for reason in reasons:
            reasons_count[reason] = reasons_count.get(reason, 0) + 1
    
    print("\nRaisons principales des anomalies:")
    for reason, count in sorted(reasons_count.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(anomalies)) * 100
        print(f"- {reason}: {percentage:.1f}% ({count} repas)")
    
    # Sauvegarder les résultats détaillés
    results_dir = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(results_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results[results['is_anomaly']].to_excel(
        os.path.join(results_dir, f'anomalies_detected_{timestamp}.xlsx'),
        index=False
    )

if __name__ == "__main__":
    # Chercher le dernier modèle entraîné
    models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models', 'trained')
    model_files = [f for f in os.listdir(models_dir) if f.startswith('isolation_forest_')]
    scaler_files = [f for f in os.listdir(models_dir) if f.startswith('scaler_')]
    
    if not model_files or not scaler_files:
        print("Aucun modèle trouvé. Veuillez d'abord entraîner le modèle.")
    else:
        latest_model = max(model_files)
        latest_scaler = max(scaler_files)
        
        model_path = os.path.join(models_dir, latest_model)
        scaler_path = os.path.join(models_dir, latest_scaler)
        
        evaluate_model(model_path, scaler_path)
