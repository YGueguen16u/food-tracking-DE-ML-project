import os
import json
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from aws_s3.connect_s3 import S3Manager
from typing import Tuple

def load_model() -> Tuple[KMeans, StandardScaler]:
    """
    Charge le modèle de clustering depuis S3.
    
    Returns:
        Tuple[KMeans, StandardScaler]: Le modèle KMeans et le scaler associé
    """
    s3_manager = S3Manager()
    model_key = "AI/clustering/models/kmeans_model.json"
    
    # Télécharger le modèle
    local_path = "/tmp/temp_model.json"
    if not s3_manager.download_file(model_key, local_path):
        raise FileNotFoundError("Modèle non trouvé dans S3")
    
    # Charger les données du modèle
    with open(local_path, 'r') as f:
        model_data = json.load(f)
    
    # Nettoyer le fichier temporaire
    os.remove(local_path)
    
    # Recréer le modèle
    kmeans = KMeans(n_clusters=model_data['n_clusters'])
    kmeans.cluster_centers_ = np.array(model_data['cluster_centers'])
    
    # Recréer le scaler
    scaler = StandardScaler()
    scaler.mean_ = np.array(model_data['scaler_mean'])
    scaler.scale_ = np.array(model_data['scaler_scale'])
    
    return kmeans, scaler
