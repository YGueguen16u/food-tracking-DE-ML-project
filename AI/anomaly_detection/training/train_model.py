# AI/anomaly_detection/training/train_model.py

"""
Script for training the anomaly detection model.

This script loads meal data, prepares it for training, and trains the anomaly detection model.
It also provides functions for loading and saving the model, as well as for making predictions.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
import json
import tempfile

# Add parent directory to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_dir)

from AI.anomaly_detection.models.anomaly_detector import MealAnomalyDetector, AnomalyAnalyzer
from AI.anomaly_detection.utils.data_loader import DataLoader
from aws_s3.connect_s3 import S3Manager

def prepare_training_data(filtered_data, daily_stats, food_proportions):
    """
    Prepare the data for training
    
    Args:
        filtered_data (pd.DataFrame): Filtered meal data
        daily_stats (pd.DataFrame): Daily statistics
        food_proportions (pd.DataFrame): Food proportions
        
    Returns:
        pd.DataFrame: Prepared data for training
    """
    # Merge with daily statistics
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
    
    # Calculate deviations from rolling averages
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
    """
    Calculate detailed model statistics.
    
    Args:
        detector (MealAnomalyDetector): Trained anomaly detector
        train_data (pd.DataFrame): Training data
        test_data (pd.DataFrame): Test data
        results_df (pd.DataFrame): Results DataFrame
        
    Returns:
        dict: Detailed model statistics
    """
    
    # General statistics
    total_samples = len(results_df)
    anomaly_samples = results_df['is_anomaly'].sum()
    normal_samples = total_samples - anomaly_samples
    
    # Nutrient statistics for anomalies
    anomaly_df = results_df[results_df['is_anomaly']]
    normal_df = results_df[~results_df['is_anomaly']]
    
    # Calculate means and standard deviations
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
    """
    Train and save the anomaly detection model
    """
    
    # Initialize S3Manager
    s3_manager = S3Manager()
    
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    
    print("Loading data...")
    loader = DataLoader()
    filtered_data, daily_stats, food_proportions = loader.load_training_data()
    
    # Check if data was loaded successfully
    if filtered_data is None or daily_stats is None or food_proportions is None:
        print("Error loading data")
        return
    
    print("Preparing data...")
    training_data = prepare_training_data(filtered_data, daily_stats, food_proportions)
    
    # Split into training and test sets
    train_data, test_data = train_test_split(
        training_data,
        test_size=0.2,
        random_state=42
    )
    
    print("Training model...")
    detector = MealAnomalyDetector(contamination=0.1)
    detector.fit(train_data)
    
    print("Evaluating model...")
    test_predictions = detector.predict(test_data)
    
    # Calculate metrics
    anomaly_rate = (test_predictions['is_anomaly'].sum() / len(test_predictions)) * 100
    print(f"Anomaly detection rate: {anomaly_rate:.2f}%")
    
    # Analyze anomalies
    analyzer = AnomalyAnalyzer()
    anomalies = test_predictions[test_predictions['is_anomaly']]
    
    print("\nExamples of detected anomalies:")
    for _, anomaly in anomalies.head().iterrows():
        reasons = analyzer.analyze_anomaly(anomaly)
        print(f"\nMeal on {anomaly['date']} at {anomaly['heure']}:")
        print(f"- Calories: {anomaly['total_calories']:.0f}")
        print(f"- Anomaly score: {anomaly['anomaly_score']:.3f}")
        print("Possible reasons:")
        for reason in reasons:
            print(f"  * {reason}")
    
    # Save results to S3
    print("\nSaving results to S3...")
    
    try:
        # 1. Save predictions
        predictions_key = "AI/anomaly_detection/results/anomalies_detected.xlsx"
        temp_predictions = os.path.join(temp_dir, "predictions.xlsx")
        with pd.ExcelWriter(temp_predictions) as writer:
            test_predictions.to_excel(writer, index=False)
        s3_manager.upload_with_overwrite(temp_predictions, predictions_key)
        
        # 2. Save statistics
        stats = calculate_model_statistics(detector, train_data, test_data, test_predictions)
        stats_key = "AI/anomaly_detection/results/model_statistics.json"
        temp_stats = os.path.join(temp_dir, "stats.json")
        with open(temp_stats, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=4, ensure_ascii=False)
        s3_manager.upload_with_overwrite(temp_stats, stats_key)
        
        # 3. Save model
        model_key = "AI/anomaly_detection/models/trained_model.joblib"
        temp_model = os.path.join(temp_dir, "model.joblib")
        detector.save(temp_model)
        s3_manager.upload_with_overwrite(temp_model, model_key)
        
        print("\nFiles saved to S3:")
        print(f"- Predictions: {predictions_key}")
        print(f"- Statistics: {stats_key}")
        print(f"- Model: {model_key}")
        
    finally:
        # Clean up temporary files
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    # Display some key statistics
    print(f"\nKey statistics:")
    print(f"- Total samples: {stats['general_statistics']['total_samples']}")
    print(f"- Anomaly rate: {stats['general_statistics']['anomaly_rate']}%")
    print(f"- Mean anomaly score: {stats['anomaly_statistics']['mean_anomaly_score']}")

if __name__ == "__main__":
    train_and_save_model()
