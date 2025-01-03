import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import boto3
import json
from datetime import datetime
from .user_user_cf import UserUserCF
from .item_item_cf import ItemItemCF

class HybridRecommender:
    def __init__(self, bucket_name: str, region_name: str = 'eu-west-3'):
        """
        Initialise le système de recommandation hybride.
        
        Args:
            bucket_name: Nom du bucket S3
            region_name: Région AWS
        """
        self.user_cf = UserUserCF(bucket_name, region_name)
        self.item_cf = ItemItemCF(bucket_name, region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name

    def initialize(self) -> None:
        """Initialise les deux systèmes de recommandation."""
        # Charger les données
        self.user_cf.load_data_from_s3()
        self.item_cf.load_data_from_s3()
        
        # Calculer les matrices de similarité
        self.user_cf.compute_user_similarity()
        self.item_cf.compute_item_similarity()

    def get_hybrid_recommendations(
        self,
        user_id: int,
        n_recommendations: int = 5,
        user_weight: float = 0.5
    ) -> List[Tuple[str, float]]:
        """
        Génère des recommandations hybrides en combinant les approches user-user et item-item.
        
        Args:
            user_id: ID de l'utilisateur
            n_recommendations: Nombre de recommandations à générer
            user_weight: Poids donné aux recommandations user-user (0-1)
            
        Returns:
            Liste de tuples (item_id, score_hybride)
        """
        # Obtenir les recommandations des deux systèmes
        user_recs = self.user_cf.get_recommendations(user_id, n_recommendations * 2)
        item_recs = self.item_cf.get_recommendations(user_id, n_recommendations * 2)
        
        # Convertir en dictionnaires pour faciliter la fusion
        user_dict = dict(user_recs)
        item_dict = dict(item_recs)
        
        # Combiner les scores
        hybrid_scores = {}
        all_items = set(user_dict.keys()) | set(item_dict.keys())
        
        for item_id in all_items:
            user_score = user_dict.get(item_id, 0)
            item_score = item_dict.get(item_id, 0)
            
            # Score hybride pondéré
            hybrid_score = (user_weight * user_score + 
                          (1 - user_weight) * item_score)
            
            hybrid_scores[item_id] = hybrid_score
        
        # Trier et retourner les top N recommandations
        sorted_items = sorted(
            hybrid_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_items[:n_recommendations]

    def get_contextual_recommendations(
        self,
        user_id: int,
        context: Dict,
        n_recommendations: int = 5
    ) -> List[Tuple[str, float]]:
        """
        Génère des recommandations contextuelles.
        
        Args:
            user_id: ID de l'utilisateur
            context: Dictionnaire contenant les informations contextuelles
                    (e.g., {'time': '12:00', 'day': 'Monday'})
            n_recommendations: Nombre de recommandations à générer
            
        Returns:
            Liste de tuples (item_id, score_ajusté)
        """
        # Obtenir les recommandations de base
        base_recommendations = self.get_hybrid_recommendations(
            user_id,
            n_recommendations * 2
        )
        
        try:
            # Charger les données contextuelles depuis S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key='processed_data/meal_context.json'
            )
            context_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Ajuster les scores en fonction du contexte
            adjusted_scores = []
            for item_id, base_score in base_recommendations:
                context_score = self._calculate_context_score(
                    item_id,
                    context,
                    context_data
                )
                adjusted_score = base_score * context_score
                adjusted_scores.append((item_id, adjusted_score))
            
            # Trier et retourner les top N recommandations
            return sorted(
                adjusted_scores,
                key=lambda x: x[1],
                reverse=True
            )[:n_recommendations]
            
        except Exception as e:
            print(f"Erreur lors du chargement des données contextuelles: {e}")
            return base_recommendations[:n_recommendations]

    def _calculate_context_score(
        self,
        item_id: str,
        current_context: Dict,
        context_data: Dict
    ) -> float:
        """
        Calcule un score contextuel pour un item.
        
        Args:
            item_id: ID de l'item
            current_context: Contexte actuel
            context_data: Données historiques de contexte
            
        Returns:
            Score contextuel (0-1)
        """
        if item_id not in context_data:
            return 1.0
            
        item_context = context_data[item_id]
        context_score = 1.0
        
        # Ajuster le score en fonction de l'heure
        if 'time' in current_context and 'time_distribution' in item_context:
            hour = int(current_context['time'].split(':')[0])
            time_score = item_context['time_distribution'].get(str(hour), 0)
            context_score *= (0.5 + 0.5 * time_score)  # Normaliser entre 0.5 et 1
            
        # Ajuster le score en fonction du jour
        if 'day' in current_context and 'day_distribution' in item_context:
            day = current_context['day']
            day_score = item_context['day_distribution'].get(day, 0)
            context_score *= (0.5 + 0.5 * day_score)  # Normaliser entre 0.5 et 1
            
        return context_score

    def save_recommendations_to_s3(
        self,
        user_id: int,
        recommendations: List[Tuple[str, float]],
        context: Dict = None
    ) -> None:
        """
        Sauvegarde les recommandations hybrides dans S3.
        
        Args:
            user_id: ID de l'utilisateur
            recommendations: Liste de tuples (item_id, score)
            context: Dictionnaire de contexte (optionnel)
        """
        timestamp = datetime.now().isoformat()
        data = {
            'user_id': user_id,
            'timestamp': timestamp,
            'recommendations': [
                {'item_id': item_id, 'score': float(score)}
                for item_id, score in recommendations
            ]
        }
        
        if context:
            data['context'] = context
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f'recommendations/user_{user_id}/{timestamp}_hybrid.json',
                Body=json.dumps(data)
            )
        except Exception as e:
            print(f"Erreur lors de la sauvegarde des recommandations: {e}")
