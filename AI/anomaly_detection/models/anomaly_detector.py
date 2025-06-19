# AI/anomaly_detection/models/anomaly_detector.py

"""
Anomaly detection model for meal data.

This module implements a meal anomaly detection system using Isolation Forest.
It provides a class for training and loading the model, as well as functions for
preparing features, fitting the model, and making predictions.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
from datetime import datetime
import os

class MealAnomalyDetector:
    """
    Anomaly detector for meal data using Isolation Forest.
    """
    
    def __init__(self, contamination=0.1):
        """
        Initialize the anomaly detector.
        
        Args:
            contamination (float): Proportion expected of anomalies in the dataset
        """
        # Initialize the Isolation Forest model
        self.model = IsolationForest(
            contamination=contamination, # Proportion expected of anomalies in the dataset
            random_state=42, # Random seed for reproducibility
            n_estimators=100 # Number of trees in the forest
        )
        self.scaler = StandardScaler()
        self.feature_columns = [
            'total_calories', 'total_lipids', 'total_carbs', 'total_protein',
            'hour', 'weekday'
        ]
        
    def prepare_features(self, df):
        """
        Prepare the features for training or prediction.
        
        Args:
            df (pd.DataFrame): DataFrame containing meal data
        
        Returns:
            np.ndarray: Array of features
        """
        # Extract hour and weekday
        df = df.copy()
        df['hour'] = pd.to_datetime(df['heure']).dt.hour # Extract hour from datetime
        df['weekday'] = pd.to_datetime(df['date']).dt.dayofweek # Extract weekday from datetime
        
        # Select features
        X = df[self.feature_columns].copy()
        
        # Normalize features
        if not hasattr(self.scaler, 'mean_'):
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)
        
        return X_scaled
    
    def fit(self, df):
        """
        Train the anomaly detection model.
        
        Args:
            df (pd.DataFrame): DataFrame containing meal data
        """
        # Prepare features
        X = self.prepare_features(df)
        
        # Fit the scaler and model
        self.model.fit(X)
        
    def predict(self, df):
        """
        Predict anomalies in new data
        
        Args:
            df (pd.DataFrame): DataFrame containing meal data
            
        Returns:
            pd.DataFrame: DataFrame containing meal data with prediction columns added
        """
        # Prepare features
        X = self.prepare_features(df)
        
        # Predict anomalies (-1 for anomaly, 1 for normal)
        predictions = self.model.predict(X)
        scores = self.model.score_samples(X)
        
        # Add predictions to DataFrame
        result_df = df.copy()
        result_df['is_anomaly'] = predictions == -1
        result_df['anomaly_score'] = scores
        
        return result_df
    
    def predict_and_explain(self, df):
        """
        Detect anomalies and provide explanations.
        
        Args:
            df (pd.DataFrame): DataFrame containing meal data
            
        Returns:
            pd.DataFrame: DataFrame containing meal data with prediction columns added
        """
        # Copy the DataFrame to avoid in-place modifications
        df_copy = df.copy()
        
        # Prepare features and prediction
        X = self.prepare_features(df_copy)
        anomaly_scores = self.model.score_samples(X)
        
        # Add scores to DataFrame
        df_copy['anomaly_score'] = anomaly_scores
        df_copy['is_anomaly'] = anomaly_scores < self.model.contamination
        
        # Add explanations for anomalies
        df_copy['explanation'] = ''
        mask_anomaly = df_copy['is_anomaly'] # Boolean mask for anomalies
        
        if mask_anomaly.any():
            # Calculate proportions
            total_nutrients = df_copy['lipides'] + df_copy['glucides'] + df_copy['proteines']
            df_copy['prop_lipides'] = df_copy['lipides'] / total_nutrients
            df_copy['prop_glucides'] = df_copy['glucides'] / total_nutrients
            df_copy['prop_proteines'] = df_copy['proteines'] / total_nutrients
            
            # Thresholds for proportions (to be adjusted according to needs)
            lipids_threshold = 0.35
            carbs_threshold = 0.65
            proteins_threshold = 0.35
            
            # Verification of different conditions for anomalies
            explanations = []
            for _, row in df_copy[mask_anomaly].iterrows():
                reasons = []
                
                if row['prop_lipides'] > lipids_threshold:
                    reasons.append("High lipids proportion")
                if row['prop_glucides'] > carbs_threshold:
                    reasons.append("High carbs proportion")
                if row['prop_proteines'] > proteins_threshold:
                    reasons.append("High proteins proportion")
                if row['hour'] < 6 or row['hour'] > 22:
                    reasons.append("Unusual hour")
                if not reasons:
                    reasons.append("Unusual combination of nutrients")
                
                df_copy.loc[df_copy['is_anomaly'] & (df_copy['heure'] == row['heure']), 'explanation'] = " | ".join(reasons)
        
        return df_copy[['user_id', 'heure', 'calories', 'lipides', 'glucides', 'proteines', 'anomaly_score', 'is_anomaly', 'explanation']]
    
    def save(self, model_path):
        """
        Save the model and scaler
        
        Args:
            model_path (str): Complete path to save the model
        """
        # Save the model
        joblib.dump(self.model, model_path)
        
        # Save the scaler in the same directory
        scaler_path = os.path.join(os.path.dirname(model_path), "scaler.joblib")
        joblib.dump(self.scaler, scaler_path)

    # decorator : load a saved model and scaler     
    @classmethod
    def load(cls, model_path, scaler_path):
        """
        Load a saved model and scaler
        
        Args:
            model_path (str): Path to the model file
            scaler_path (str): Path to the scaler file
            
        Returns:
            MealAnomalyDetector: Instance avec le modèle chargé
        """
        detector = cls() # Create a new instance of MealAnomalyDetector
        detector.model = joblib.load(model_path) # Load the model
        detector.scaler = joblib.load(scaler_path) # Load the scaler
        return detector 


class AnomalyAnalyzer:
    """
    Analyze anomalies and provide explanations
    """
    
    def __init__(self):
        """Initialize the anomaly analyzer

        Args:
            thresholds (dict): Dictionary of thresholds for different metrics
        """
        # Thresholds for different metrics (to be adjusted according to needs)
        self.thresholds = {
            'calories_high': 3000,  # calories per meal
            'calories_low': 100,
            'lipids_ratio': 0.35,   # % of total calories
            'carbs_ratio': 0.65,
            'protein_ratio': 0.35,
            'meal_time': {
                'breakfast': (5, 10),
                'lunch': (11, 14),
                'dinner': (18, 21)
            }
        }
    
    def analyze_anomaly(self, meal_data):
        """
        Analyze an anomaly detected in a meal and provide explanations
        
        Args:
            meal_data (pd.Series): Meal data
            
        Returns:
            list: List of possible reasons for the anomaly
        """
        reasons = []
        
        # Verify total calories
        if meal_data['total_calories'] > self.thresholds['calories_high']:
            reasons.append("High total calories")
        elif meal_data['total_calories'] < self.thresholds['calories_low']:
            reasons.append("Low total calories")
        
        # Verify macronutrient ratios
        calories_from_lipids = meal_data['total_lipids'] * 9
        calories_from_carbs = meal_data['total_carbs'] * 4
        calories_from_protein = meal_data['total_protein'] * 4
        
        lipids_ratio = calories_from_lipids / meal_data['total_calories']
        carbs_ratio = calories_from_carbs / meal_data['total_calories']
        protein_ratio = calories_from_protein / meal_data['total_calories']
        
        if lipids_ratio > self.thresholds['lipids_ratio']:
            reasons.append("High lipids ratio")
        if carbs_ratio > self.thresholds['carbs_ratio']:
            reasons.append("High carbs ratio")
        if protein_ratio > self.thresholds['protein_ratio']:
            reasons.append("High protein ratio")
        
        # Verify meal time
        hour = pd.to_datetime(meal_data['heure']).hour
        meal_time_normal = False
        for meal, (start, end) in self.thresholds['meal_time'].items():
            if start <= hour <= end:
                meal_time_normal = True
                break
        
        if not meal_time_normal:
            reasons.append("Unusual meal time")
        
        return reasons
