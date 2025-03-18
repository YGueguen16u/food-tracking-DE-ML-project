import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
from datetime import datetime
import os

class MealAnomalyDetector:
    """Détecteur d'anomalies pour les repas basé sur Isolation Forest"""
    
    def __init__(self, contamination=0.1):
        """
        Initialise le détecteur d'anomalies
        
        Args:
            contamination (float): Proportion attendue d'anomalies dans le dataset
        """
        self.model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.feature_columns = [
            'total_calories', 'total_lipids', 'total_carbs', 'total_protein',
            'hour', 'weekday'
        ]
        
    def prepare_features(self, df):
        """Prépare les caractéristiques pour l'entraînement ou la prédiction."""
        # Extraire l'heure et le jour de la semaine
        df = df.copy()
        df['hour'] = pd.to_datetime(df['heure']).dt.hour
        df['weekday'] = pd.to_datetime(df['date']).dt.dayofweek
        
        # Sélectionner les features
        X = df[self.feature_columns].copy()
        
        # Normalisation des features
        if not hasattr(self.scaler, 'mean_'):
            X_scaled = self.scaler.fit_transform(X)
        else:
            X_scaled = self.scaler.transform(X)
        
        return X_scaled
    
    def fit(self, df):
        """
        Entraîne le modèle de détection d'anomalies
        
        Args:
            df (pd.DataFrame): DataFrame d'entraînement
        """
        # Préparer les features
        X = self.prepare_features(df)
        
        # Fit le scaler et le modèle
        self.model.fit(X)
        
    def predict(self, df):
        """
        Prédit les anomalies dans les nouvelles données
        
        Args:
            df (pd.DataFrame): DataFrame à analyser
            
        Returns:
            pd.DataFrame: DataFrame original avec les colonnes de prédiction ajoutées
        """
        # Préparer les features
        X = self.prepare_features(df)
        
        # Prédire les anomalies (-1 pour anomalie, 1 pour normal)
        predictions = self.model.predict(X)
        scores = self.model.score_samples(X)
        
        # Ajouter les prédictions au DataFrame
        result_df = df.copy()
        result_df['is_anomaly'] = predictions == -1
        result_df['anomaly_score'] = scores
        
        return result_df
    
    def predict_and_explain(self, df):
        """Détecte les anomalies et fournit des explications."""
        # Copie du DataFrame pour éviter les modifications en place
        df_copy = df.copy()
        
        # Préparation des features et prédiction
        X = self.prepare_features(df_copy)
        anomaly_scores = self.model.score_samples(X)
        
        # Ajout des scores au DataFrame
        df_copy['anomaly_score'] = anomaly_scores
        df_copy['is_anomaly'] = anomaly_scores < self.model.contamination
        
        # Ajout des explications pour les anomalies
        df_copy['explanation'] = ''
        mask_anomaly = df_copy['is_anomaly']
        
        if mask_anomaly.any():
            # Calcul des proportions
            total_nutrients = df_copy['lipides'] + df_copy['glucides'] + df_copy['proteines']
            df_copy['prop_lipides'] = df_copy['lipides'] / total_nutrients
            df_copy['prop_glucides'] = df_copy['glucides'] / total_nutrients
            df_copy['prop_proteines'] = df_copy['proteines'] / total_nutrients
            
            # Seuils pour les proportions (à ajuster selon les besoins)
            lipids_threshold = 0.35
            carbs_threshold = 0.65
            proteins_threshold = 0.35
            
            # Vérification des différentes conditions pour les anomalies
            explanations = []
            for _, row in df_copy[mask_anomaly].iterrows():
                reasons = []
                
                if row['prop_lipides'] > lipids_threshold:
                    reasons.append("Proportion de lipides élevée")
                if row['prop_glucides'] > carbs_threshold:
                    reasons.append("Proportion de glucides élevée")
                if row['prop_proteines'] > proteins_threshold:
                    reasons.append("Proportion de protéines élevée")
                if row['hour'] < 6 or row['hour'] > 22:
                    reasons.append("Heure inhabituelle")
                if not reasons:
                    reasons.append("Combinaison inhabituelle de nutriments")
                
                df_copy.loc[df_copy['is_anomaly'] & (df_copy['heure'] == row['heure']), 'explanation'] = " | ".join(reasons)
        
        return df_copy[['user_id', 'heure', 'calories', 'lipides', 'glucides', 'proteines', 'anomaly_score', 'is_anomaly', 'explanation']]
    
    def save(self, model_path):
        """
        Sauvegarde le modèle et le scaler
        
        Args:
            model_path (str): Chemin complet où sauvegarder le modèle
        """
        # Sauvegarder le modèle
        joblib.dump(self.model, model_path)
        
        # Sauvegarder le scaler dans le même dossier
        scaler_path = os.path.join(os.path.dirname(model_path), "scaler.joblib")
        joblib.dump(self.scaler, scaler_path)
        
    @classmethod
    def load(cls, model_path, scaler_path):
        """
        Charge un modèle et un scaler sauvegardés
        
        Args:
            model_path (str): Chemin vers le fichier du modèle
            scaler_path (str): Chemin vers le fichier du scaler
            
        Returns:
            MealAnomalyDetector: Instance avec le modèle chargé
        """
        detector = cls()
        detector.model = joblib.load(model_path)
        detector.scaler = joblib.load(scaler_path)
        return detector


class AnomalyAnalyzer:
    """Analyse les anomalies détectées et fournit des explications"""
    
    def __init__(self):
        """Initialise l'analyseur d'anomalies"""
        # Seuils pour les différentes métriques (à ajuster selon les besoins)
        self.thresholds = {
            'calories_high': 3000,  # calories par repas
            'calories_low': 100,
            'lipids_ratio': 0.35,   # % des calories totales
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
        Analyse un repas détecté comme anomalie et fournit des explications
        
        Args:
            meal_data (pd.Series): Données d'un repas
            
        Returns:
            list: Liste des raisons possibles de l'anomalie
        """
        reasons = []
        
        # Vérifier les calories totales
        if meal_data['total_calories'] > self.thresholds['calories_high']:
            reasons.append("Calories anormalement élevées")
        elif meal_data['total_calories'] < self.thresholds['calories_low']:
            reasons.append("Calories anormalement basses")
        
        # Vérifier les ratios de macronutriments
        calories_from_lipids = meal_data['total_lipids'] * 9
        calories_from_carbs = meal_data['total_carbs'] * 4
        calories_from_protein = meal_data['total_protein'] * 4
        
        lipids_ratio = calories_from_lipids / meal_data['total_calories']
        carbs_ratio = calories_from_carbs / meal_data['total_calories']
        protein_ratio = calories_from_protein / meal_data['total_calories']
        
        if lipids_ratio > self.thresholds['lipids_ratio']:
            reasons.append("Proportion de lipides trop élevée")
        if carbs_ratio > self.thresholds['carbs_ratio']:
            reasons.append("Proportion de glucides trop élevée")
        if protein_ratio > self.thresholds['protein_ratio']:
            reasons.append("Proportion de protéines trop élevée")
        
        # Vérifier l'heure du repas
        hour = pd.to_datetime(meal_data['heure']).hour
        meal_time_normal = False
        for meal, (start, end) in self.thresholds['meal_time'].items():
            if start <= hour <= end:
                meal_time_normal = True
                break
        
        if not meal_time_normal:
            reasons.append("Heure inhabituelle pour un repas")
        
        return reasons
