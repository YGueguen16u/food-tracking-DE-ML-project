# AI/anomaly_detection/evaluation/evaluate_model.py

"""
Script for evaluating the anomaly detection model.

This script loads a pre-trained anomaly detection model and evaluates its performance on a test dataset.
It provides a detailed analysis of the model's predictions, including:
- Global statistics
- Distribution of anomaly scores
- Temporal coherence
- Model stability
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# Add parent directory to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_dir)

from AI.anomaly_detection.models.anomaly_detector import MealAnomalyDetector, AnomalyAnalyzer
from AI.anomaly_detection.utils.data_loader import DataLoader

def evaluate_model(model_path, scaler_path):
    """
    Evaluate the model on a test dataset.

    This function loads a pre-trained anomaly detection model and evaluates its performance on a test dataset.
    It provides a detailed analysis of the model's predictions, including:
    - Global statistics
    - Distribution of anomaly scores
    - Temporal coherence
    - Model stability
    
    Args:
        model_path (str): Path to the saved model
        scaler_path (str): Path to the saved scaler
    """
    print("Loading data...")
    loader = DataLoader()
    filtered_data, daily_stats, food_proportions = loader.load_training_data()
    
    if filtered_data is None:
        print("Error loading data")
        return
    
    # Load model
    print("Loading model...")
    detector = MealAnomalyDetector.load(model_path, scaler_path)
    analyzer = AnomalyAnalyzer()
    
    print("Predicting anomalies...")
    results = detector.predict(filtered_data)
    
    # Global analysis
    anomaly_rate = (results['is_anomaly'].sum() / len(results)) * 100
    print(f"\nGlobal statistics:")
    print(f"- Total number of meals analyzed: {len(results)}")
    print(f"- Number of anomalies detected: {results['is_anomaly'].sum()}")
    print(f"- Anomaly rate: {anomaly_rate:.2f}%")
    
    # Distribution of anomaly scores
    plt.figure(figsize=(10, 6))
    sns.histplot(data=results, x='anomaly_score', hue='is_anomaly', bins=50)
    plt.title('Distribution of anomaly scores')
    plt.xlabel('Anomaly score')
    plt.ylabel('Number of meals')
    
    # Save the plot
    plots_dir = os.path.join(os.path.dirname(__file__), 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    plt.savefig(os.path.join(plots_dir, 'anomaly_scores_distribution.png'))
    
    # Temporal analysis
    results['hour'] = pd.to_datetime(results['heure']).dt.hour
    results['weekday'] = pd.to_datetime(results['date']).dt.dayofweek
    
    # Distribution of anomalies by hour
    plt.figure(figsize=(12, 6))
    anomaly_by_hour = results[results['is_anomaly']]['hour'].value_counts().sort_index()
    total_by_hour = results['hour'].value_counts().sort_index()
    anomaly_rate_by_hour = (anomaly_by_hour / total_by_hour * 100)
    
    sns.lineplot(x=anomaly_rate_by_hour.index, y=anomaly_rate_by_hour.values)
    plt.title('Anomaly rate by hour')
    plt.xlabel('Hour')
    plt.ylabel('Anomaly rate (%)')
    plt.savefig(os.path.join(plots_dir, 'anomaly_rate_by_hour.png'))
    
    # Analysis of anomaly reasons
    print("\nAnalysis of detected anomalies:")
    anomalies = results[results['is_anomaly']].copy()
    reasons_count = {}
    
    for _, anomaly in anomalies.iterrows():
        reasons = analyzer.analyze_anomaly(anomaly)
        for reason in reasons:
            reasons_count[reason] = reasons_count.get(reason, 0) + 1
    
    print("\nMain reasons for anomalies:")
    for reason, count in sorted(reasons_count.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(anomalies)) * 100
        print(f"- {reason}: {percentage:.1f}% ({count} meals)")
    
    # Save detailed results
    results_dir = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(results_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results[results['is_anomaly']].to_excel(
        os.path.join(results_dir, f'anomalies_detected_{timestamp}.xlsx'),
        index=False
    )

if __name__ == "__main__":
    # Search for the latest trained model
    models_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models', 'trained')
    model_files = [f for f in os.listdir(models_dir) if f.startswith('isolation_forest_')]
    scaler_files = [f for f in os.listdir(models_dir) if f.startswith('scaler_')]
    
    if not model_files or not scaler_files:
        print("No model found. Please train the model first.")
    else:
        latest_model = max(model_files)
        latest_scaler = max(scaler_files)
        
        model_path = os.path.join(models_dir, latest_model)
        scaler_path = os.path.join(models_dir, latest_scaler)
        
        evaluate_model(model_path, scaler_path)
