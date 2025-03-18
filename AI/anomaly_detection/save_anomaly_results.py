import sys
import os
import json
import pandas as pd
from datetime import datetime

# Ajouter les répertoires nécessaires au PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.extend([current_dir, project_root])

from aws_s3.connect_s3 import S3Manager
from AI.anomaly_detection.models.anomaly_detector import MealAnomalyDetector, AnomalyAnalyzer
from AI.anomaly_detection.utils.data_loader import DataLoader

def save_anomaly_results():
    """Entraîne le modèle de détection d'anomalies et sauvegarde les résultats dans S3"""
    
    # Initialiser S3Manager
    s3_manager = S3Manager()
    
    print("Chargement des données...")
    loader = DataLoader()
    filtered_data, daily_stats, food_proportions = loader.load_training_data()
    
    if filtered_data is None or daily_stats is None or food_proportions is None:
        print("Erreur lors du chargement des données")
        return
        
    # Nettoyer les données numériques
    print("Nettoyage des données...")
    numeric_columns = ['total_calories', 'total_lipids', 'total_carbs', 'total_protein', 'quantity']
    
    for col in numeric_columns:
        # Convertir en chaîne et nettoyer les valeurs problématiques
        filtered_data[col] = filtered_data[col].astype(str).str.replace(r'(\d+\.\d+)\1+', r'\1', regex=True)
        # Convertir en float
        filtered_data[col] = pd.to_numeric(filtered_data[col], errors='coerce')
    
    # Supprimer les lignes avec des valeurs manquantes
    filtered_data = filtered_data.dropna(subset=numeric_columns)
    
    print(f"Nombre de lignes après nettoyage : {len(filtered_data)}")
    
    print("Entraînement du modèle...")
    detector = MealAnomalyDetector(contamination=0.1)
    detector.fit(filtered_data)
    
    # Faire des prédictions sur l'ensemble des données
    print("Détection des anomalies...")
    predictions = detector.predict(filtered_data)
    
    # Analyser les anomalies
    print("Analyse des anomalies...")
    analyzer = AnomalyAnalyzer()
    anomalies = predictions[predictions['is_anomaly']]
    
    # Préparer l'analyse détaillée des anomalies
    anomaly_analysis = {
        'general_stats': {
            'total_meals': len(predictions),
            'anomaly_count': len(anomalies),
            'anomaly_rate': (len(anomalies) / len(predictions)) * 100,
            'mean_anomaly_score': predictions['anomaly_score'].mean(),
            'min_anomaly_score': predictions['anomaly_score'].min(),
            'max_anomaly_score': predictions['anomaly_score'].max()
        },
        'anomalies': []
    }
    
    # Analyser chaque anomalie
    for _, anomaly in anomalies.iterrows():
        reasons = analyzer.analyze_anomaly(anomaly)
        anomaly_analysis['anomalies'].append({
            'date': str(anomaly['date']),
            'heure': str(anomaly['heure']),
            'user_id': str(anomaly['user_id']),
            'total_calories': float(anomaly['total_calories']),
            'total_lipids': float(anomaly['total_lipids']),
            'total_carbs': float(anomaly['total_carbs']),
            'total_protein': float(anomaly['total_protein']),
            'anomaly_score': float(anomaly['anomaly_score']),
            'reasons': reasons
        })
    
    # Préparer les statistiques temporelles
    time_stats = predictions[predictions['is_anomaly']].groupby(
        pd.to_datetime(predictions['heure']).dt.hour
    ).size().reset_index()
    time_stats.columns = ['hour', 'anomaly_count']
    anomaly_analysis['time_distribution'] = time_stats.to_dict('records')
    
    # Sauvegarder les résultats dans S3
    print("Sauvegarde des résultats dans S3...")
    
    # 1. Sauvegarder les prédictions complètes
    predictions_key = "AI/anomaly_detection/results/meal_anomalies.xlsx"
    with pd.ExcelWriter('/tmp/temp_predictions.xlsx') as writer:
        predictions.to_excel(writer, index=False)
    s3_manager.upload_with_overwrite('/tmp/temp_predictions.xlsx', predictions_key)
    os.remove('/tmp/temp_predictions.xlsx')
    
    # 2. Sauvegarder l'analyse détaillée
    analysis_key = "AI/anomaly_detection/results/anomaly_analysis.json"
    with open('/tmp/temp_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(anomaly_analysis, f, ensure_ascii=False, indent=4)
    s3_manager.upload_with_overwrite('/tmp/temp_analysis.json', analysis_key)
    os.remove('/tmp/temp_analysis.json')
    
    # 3. Sauvegarder le modèle
    model_key = "AI/anomaly_detection/models/trained_model.joblib"
    detector.save('/tmp/temp_model.joblib')
    s3_manager.upload_with_overwrite('/tmp/temp_model.joblib', model_key)
    os.remove('/tmp/temp_model.joblib')
    
    print("\nRésultats sauvegardés dans S3:")
    print(f"- Prédictions: {predictions_key}")
    print(f"- Analyse: {analysis_key}")
    print(f"- Modèle: {model_key}")
    
    # Afficher quelques statistiques
    print("\nStatistiques générales:")
    print(f"- Nombre total de repas analysés: {anomaly_analysis['general_stats']['total_meals']}")
    print(f"- Nombre d'anomalies détectées: {anomaly_analysis['general_stats']['anomaly_count']}")
    print(f"- Taux d'anomalies: {anomaly_analysis['general_stats']['anomaly_rate']:.2f}%")
    print(f"- Score moyen d'anomalie: {anomaly_analysis['general_stats']['mean_anomaly_score']:.3f}")

if __name__ == "__main__":
    save_anomaly_results()
