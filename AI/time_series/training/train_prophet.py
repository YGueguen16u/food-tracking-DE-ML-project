"""
Entraînement du modèle Prophet pour les prédictions de calories.
"""
import pandas as pd
from fbprophet import Prophet

def train_prophet_model(data, target_col='total_calories'):
    """
    Entraîne un modèle Prophet sur les données temporelles.
    
    Args:
        data (pd.DataFrame): Données d'entraînement
        target_col (str): Colonne cible à prédire
    
    Returns:
        Prophet: Modèle entraîné
    """
    pass  # À implémenter
