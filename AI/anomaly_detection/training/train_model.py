import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split

# Ajouter le chemin racine au PYTHONPATH
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_dir)

from AI.anomaly_detection.models.anomaly_detector import MealAnomalyDetector, AnomalyAnalyzer
from AI.anomaly_detection.utils.data_loader import DataLoader

def prepare_training_data(filtered_data, daily_stats, food_proportions):
    """
    Prépare les données pour l'entraînement
    
    Args:
        filtered_data (pd.DataFrame): Données filtrées des repas
        daily_stats (pd.DataFrame): Statistiques quotidiennes
        food_proportions (pd.DataFrame): Proportions par type d'aliment
        
    Returns:
        pd.DataFrame: Données préparées pour l'entraînement
    """
    # Fusionner avec les statistiques quotidiennes
    merged_data = pd.merge(
        filtered_data,
        daily_stats[['date', 'rolling_avg_total_calories', 'rolling_avg_total_lipids',
                    'rolling_avg_total_carbs', 'rolling_avg_total_protein']],
        on='date',
        how='left'
    )
    
    # Convert numeric columns to float
    numeric_columns = ['total_calories', 'total_lipids', 'total_carbs', 'total_protein',
                      'rolling_avg_total_calories', 'rolling_avg_total_lipids',
                      'rolling_avg_total_carbs', 'rolling_avg_total_protein']
    for col in numeric_columns:
        merged_data[col] = pd.to_numeric(merged_data[col], errors='coerce')
    
    # Calculer les écarts par rapport aux moyennes mobiles
    merged_data['calories_deviation'] = (
        merged_data['total_calories'] - merged_data['rolling_avg_total_calories']
    ) / merged_data['rolling_avg_total_calories']
    
    merged_data['lipids_deviation'] = (
        merged_data['total_lipids'] - merged_data['rolling_avg_total_lipids']
    ) / merged_data['rolling_avg_total_lipids']
    
    merged_data['carbs_deviation'] = (
        merged_data['total_carbs'] - merged_data['rolling_avg_total_carbs']
    ) / merged_data['rolling_avg_total_carbs']
    
    merged_data['protein_deviation'] = (
        merged_data['total_protein'] - merged_data['rolling_avg_total_protein']
    ) / merged_data['rolling_avg_total_protein']
    
    return merged_data

def calculate_model_statistics(detector, train_data, test_data, results_df):
    """Calcule les statistiques détaillées du modèle."""
    
    # Statistiques générales
    total_samples = len(results_df)
    anomaly_samples = results_df['is_anomaly'].sum()
    normal_samples = total_samples - anomaly_samples
    
    # Statistiques par type de nutriment pour les anomalies
    anomaly_df = results_df[results_df['is_anomaly']]
    normal_df = results_df[~results_df['is_anomaly']]
    
    # Calcul des moyennes et écarts-types
    stats = {
        "general_statistics": {
            "total_samples": int(total_samples),
            "anomaly_samples": int(anomaly_samples),
            "normal_samples": int(normal_samples),
            "anomaly_rate": float(f"{(anomaly_samples/total_samples)*100:.2f}"),
            "training_samples": len(train_data),
            "test_samples": len(test_data)
        },
        "anomaly_statistics": {
            "mean_anomaly_score": float(f"{results_df['anomaly_score'].mean():.3f}"),
            "min_anomaly_score": float(f"{results_df['anomaly_score'].min():.3f}"),
            "max_anomaly_score": float(f"{results_df['anomaly_score'].max():.3f}")
        },
        "nutrient_statistics": {
            "anomalies": {
                "total_calories": {
                    "mean": float(f"{anomaly_df['total_calories'].mean():.2f}"),
                    "std": float(f"{anomaly_df['total_calories'].std():.2f}"),
                    "min": float(f"{anomaly_df['total_calories'].min():.2f}"),
                    "max": float(f"{anomaly_df['total_calories'].max():.2f}")
                },
                "total_lipids": {
                    "mean": float(f"{anomaly_df['total_lipids'].mean():.2f}"),
                    "std": float(f"{anomaly_df['total_lipids'].std():.2f}"),
                    "min": float(f"{anomaly_df['total_lipids'].min():.2f}"),
                    "max": float(f"{anomaly_df['total_lipids'].max():.2f}")
                },
                "total_carbs": {
                    "mean": float(f"{anomaly_df['total_carbs'].mean():.2f}"),
                    "std": float(f"{anomaly_df['total_carbs'].std():.2f}"),
                    "min": float(f"{anomaly_df['total_carbs'].min():.2f}"),
                    "max": float(f"{anomaly_df['total_carbs'].max():.2f}")
                },
                "total_protein": {
                    "mean": float(f"{anomaly_df['total_protein'].mean():.2f}"),
                    "std": float(f"{anomaly_df['total_protein'].std():.2f}"),
                    "min": float(f"{anomaly_df['total_protein'].min():.2f}"),
                    "max": float(f"{anomaly_df['total_protein'].max():.2f}")
                }
            },
            "normal": {
                "total_calories": {
                    "mean": float(f"{normal_df['total_calories'].mean():.2f}"),
                    "std": float(f"{normal_df['total_calories'].std():.2f}"),
                    "min": float(f"{normal_df['total_calories'].min():.2f}"),
                    "max": float(f"{normal_df['total_calories'].max():.2f}")
                },
                "total_lipids": {
                    "mean": float(f"{normal_df['total_lipids'].mean():.2f}"),
                    "std": float(f"{normal_df['total_lipids'].std():.2f}"),
                    "min": float(f"{normal_df['total_lipids'].min():.2f}"),
                    "max": float(f"{normal_df['total_lipids'].max():.2f}")
                },
                "total_carbs": {
                    "mean": float(f"{normal_df['total_carbs'].mean():.2f}"),
                    "std": float(f"{normal_df['total_carbs'].std():.2f}"),
                    "min": float(f"{normal_df['total_carbs'].min():.2f}"),
                    "max": float(f"{normal_df['total_carbs'].max():.2f}")
                },
                "total_protein": {
                    "mean": float(f"{normal_df['total_protein'].mean():.2f}"),
                    "std": float(f"{normal_df['total_protein'].std():.2f}"),
                    "min": float(f"{normal_df['total_protein'].min():.2f}"),
                    "max": float(f"{normal_df['total_protein'].max():.2f}")
                }
            }
        },
        "model_info": {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "contamination_factor": detector.model.contamination,
            "random_state": 42
        }
    }
    
    return stats

def train_and_save_model():
    """Entraîne et sauvegarde le modèle de détection d'anomalies"""
    
    print("Chargement des données...")
    loader = DataLoader()
    filtered_data, daily_stats, food_proportions = loader.load_training_data()
    
    if filtered_data is None or daily_stats is None or food_proportions is None:
        print("Erreur lors du chargement des données")
        return
    
    print("Préparation des données...")
    training_data = prepare_training_data(filtered_data, daily_stats, food_proportions)
    
    # Diviser en ensembles d'entraînement et de test
    train_data, test_data = train_test_split(
        training_data,
        test_size=0.2,
        random_state=42
    )
    
    print("Entraînement du modèle...")
    detector = MealAnomalyDetector(contamination=0.1)
    detector.fit(train_data)
    
    # Créer le dossier pour les modèles s'il n'existe pas
    models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models', 'trained')
    os.makedirs(models_dir, exist_ok=True)
    
    print("Sauvegarde du modèle...")
    detector.save(models_dir)
    
    print("Évaluation du modèle...")
    test_predictions = detector.predict(test_data)
    
    # Calculer quelques métriques
    anomaly_rate = (test_predictions['is_anomaly'].sum() / len(test_predictions)) * 100
    print(f"Taux d'anomalies détectées: {anomaly_rate:.2f}%")
    
    # Analyser quelques anomalies
    analyzer = AnomalyAnalyzer()
    anomalies = test_predictions[test_predictions['is_anomaly']]
    
    print("\nExemples d'anomalies détectées:")
    for _, anomaly in anomalies.head().iterrows():
        reasons = analyzer.analyze_anomaly(anomaly)
        print(f"\nRepas du {anomaly['date']} à {anomaly['heure']}:")
        print(f"- Calories: {anomaly['total_calories']:.0f}")
        print(f"- Score d'anomalie: {anomaly['anomaly_score']:.3f}")
        print("Raisons possibles:")
        for reason in reasons:
            print(f"  * {reason}")
    
    # Sauvegarde des résultats dans un fichier CSV
    results_path = os.path.join(os.path.dirname(__file__), "..", "results")
    os.makedirs(results_path, exist_ok=True)
    results_file = os.path.join(results_path, "anomalies_detected.csv")
    
    # Tri des résultats par score d'anomalie (les plus anormaux en premier)
    test_predictions = test_predictions.sort_values('anomaly_score', ascending=True)
    test_predictions.to_csv(results_file, index=False, encoding='utf-8')
    print(f"\nLes résultats détaillés ont été sauvegardés dans: {results_file}")
    
    # Calcul et sauvegarde des statistiques
    stats = calculate_model_statistics(detector, train_data, test_data, test_predictions)
    
    # Sauvegarde des statistiques au format JSON
    stats_file = os.path.join(results_path, "model_statistics.json")
    import json
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=4, ensure_ascii=False)
    print(f"\nLes statistiques du modèle ont été sauvegardées dans: {stats_file}")
    
    # Affichage de quelques statistiques clés
    print(f"\nStatistiques clés:")
    print(f"- Nombre total d'échantillons: {stats['general_statistics']['total_samples']}")
    print(f"- Taux d'anomalies: {stats['general_statistics']['anomaly_rate']}%")
    print(f"- Score moyen d'anomalie: {stats['anomaly_statistics']['mean_anomaly_score']}")

if __name__ == "__main__":
    train_and_save_model()
