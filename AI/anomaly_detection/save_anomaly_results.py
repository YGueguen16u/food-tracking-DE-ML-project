# save_anomaly_results.py

"""
Script for saving anomaly detection results.

This script loads meal data, prepares it for training, and trains the anomaly detection model.
It also provides functions for loading and saving the model, as well as for making predictions.
"""

import sys
import os
import json
import pandas as pd
from datetime import datetime

# add the necessary directories to PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.extend([current_dir, project_root])

from aws_s3.connect_s3 import S3Manager
from AI.anomaly_detection.models.anomaly_detector import MealAnomalyDetector, AnomalyAnalyzer
from AI.anomaly_detection.utils.data_loader import DataLoader

def save_anomaly_results():
    """
    Train the anomaly detection model and save the results to S3
    """
    
    # Initialize S3Manager
    s3_manager = S3Manager()
    
    print("Loading data...")
    loader = DataLoader()
    filtered_data, daily_stats, food_proportions = loader.load_training_data()
    
    if filtered_data is None or daily_stats is None or food_proportions is None:
        print("Error loading data")
        return
        
    # Clean numeric data
    print("Cleaning data...")
    numeric_columns = ['total_calories', 'total_lipids', 'total_carbs', 'total_protein', 'quantity']
    
    for col in numeric_columns:
        # Convert to string and clean problematic values
        filtered_data[col] = filtered_data[col].astype(str).str.replace(r'(\d+\.\d+)\1+', r'\1', regex=True)
        # Convert to float
        filtered_data[col] = pd.to_numeric(filtered_data[col], errors='coerce')
    
    # Remove rows with missing values
    filtered_data = filtered_data.dropna(subset=numeric_columns)
    
    print(f"Number of rows after cleaning: {len(filtered_data)}")
    
    print("Training the model...")
    detector = MealAnomalyDetector(contamination=0.1)
    detector.fit(filtered_data)
    
    # Make predictions on the entire dataset
    print("Detection of anomalies...")
    predictions = detector.predict(filtered_data)
    
    # Analyze anomalies
    print("Analyze anomalies...")
    analyzer = AnomalyAnalyzer()
    anomalies = predictions[predictions['is_anomaly']]
    
    # Prepare detailed anomaly analysis
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
    
    # Analyze each anomaly
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
    
    # Prepare temporal statistics
    time_stats = predictions[predictions['is_anomaly']].groupby(
        pd.to_datetime(predictions['heure']).dt.hour
    ).size().reset_index()
    time_stats.columns = ['hour', 'anomaly_count']
    anomaly_analysis['time_distribution'] = time_stats.to_dict('records')
    
    # Save results to S3
    print("Saving results to S3...")
    
    # 1. Save complete predictions
    predictions_key = "AI/anomaly_detection/results/meal_anomalies.xlsx"
    with pd.ExcelWriter('/tmp/temp_predictions.xlsx') as writer:
        predictions.to_excel(writer, index=False)
    s3_manager.upload_with_overwrite('/tmp/temp_predictions.xlsx', predictions_key)
    os.remove('/tmp/temp_predictions.xlsx')
    
    # 2. Save detailed analysis
    analysis_key = "AI/anomaly_detection/results/anomaly_analysis.json"
    with open('/tmp/temp_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(anomaly_analysis, f, ensure_ascii=False, indent=4)
    s3_manager.upload_with_overwrite('/tmp/temp_analysis.json', analysis_key)
    os.remove('/tmp/temp_analysis.json')
    
    # 3. Save the model
    model_key = "AI/anomaly_detection/models/trained_model.joblib"
    detector.save('/tmp/temp_model.joblib')
    s3_manager.upload_with_overwrite('/tmp/temp_model.joblib', model_key)
    os.remove('/tmp/temp_model.joblib')
    
    print("\nResults saved to S3:")
    print(f"- Predictions: {predictions_key}")
    print(f"- Analysis: {analysis_key}")
    print(f"- Model: {model_key}")
    
    # Display some statistics
    print("\nGeneral statistics:")
    print(f"- Total meals analyzed: {anomaly_analysis['general_stats']['total_meals']}")
    print(f"- Anomaly count: {anomaly_analysis['general_stats']['anomaly_count']}")
    print(f"- Anomaly rate: {anomaly_analysis['general_stats']['anomaly_rate']:.2f}%")
    print(f"- Mean anomaly score: {anomaly_analysis['general_stats']['mean_anomaly_score']:.3f}")

if __name__ == "__main__":
    save_anomaly_results()
