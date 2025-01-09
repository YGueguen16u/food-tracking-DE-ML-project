"""
Page Streamlit pour le recommandeur basé sur le contenu
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import sys
from datetime import datetime

# Ajouter le chemin racine au PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from AI.recommender.content_based.food_recommender import ContentBasedRecommender
from AI.recommender.content_based.data_loader import ContentBasedDataLoader
from aws_s3.connect_s3 import S3Manager

# Configuration de la page
st.set_page_config(
    page_title="IM - Content Based Recommender",
    page_icon="📊",
    layout="wide"
)

def load_model_stats():
    """Charge les statistiques du modèle depuis S3"""
    try:
        s3_manager = S3Manager()
        response = s3_manager.s3_client.get_object(
            Bucket=s3_manager.bucket_name,
            Key='AI/recommender/content_based/results/stats.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"Erreur lors du chargement des statistiques : {str(e)}")
        return None

def load_example_recommendations():
    """Charge les recommandations d'exemple depuis S3"""
    try:
        s3_manager = S3Manager()
        response = s3_manager.s3_client.get_object(
            Bucket=s3_manager.bucket_name,
            Key='AI/recommender/content_based/results/recommendations.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"Erreur lors du chargement des recommandations : {str(e)}")
        return None

def plot_feature_distributions(food_features):
    """Crée des visualisations des distributions des caractéristiques"""
    numeric_cols = ['calories', 'lipides', 'glucides', 'proteines', 'fibres', 'sucres', 'sodium']
    
    # Créer une figure avec subplots
    fig = go.Figure()
    
    for col in numeric_cols:
        # Ajouter un violin plot pour chaque caractéristique
        fig.add_trace(go.Violin(
            y=food_features[col],
            name=col.capitalize(),
            box_visible=True,
            meanline_visible=True
        ))
    
    fig.update_layout(
        title="Distribution des caractéristiques nutritionnelles",
        yaxis_title="Valeur",
        showlegend=False,
        height=500
    )
    
    return fig

def plot_food_type_distribution(food_features):
    """Crée un graphique de la distribution des types d'aliments"""
    type_counts = food_features['Type'].value_counts()
    
    fig = px.pie(
        values=type_counts.values,
        names=type_counts.index,
        title="Distribution des types d'aliments"
    )
    
    fig.update_layout(height=500)
    return fig

def plot_feature_correlations(food_features):
    """Crée une heatmap des corrélations entre caractéristiques"""
    numeric_cols = ['calories', 'lipides', 'glucides', 'proteines', 'fibres', 'sucres', 'sodium']
    corr_matrix = food_features[numeric_cols].corr()
    
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix,
        x=numeric_cols,
        y=numeric_cols,
        colorscale='RdBu',
        zmid=0
    ))
    
    fig.update_layout(
        title="Corrélations entre les caractéristiques nutritionnelles",
        height=500
    )
    
    return fig

def plot_metrics_evolution():
    """Crée un graphique de l'évolution des métriques"""
    # TODO: Implémenter quand nous aurons des données historiques
    pass

def main():
    st.title("📊 Recommandeur Basé sur le Contenu")
    
    # Charger les données et statistiques
    data_loader = ContentBasedDataLoader()
    food_features, user_preferences = data_loader.load_training_data()
    model_stats = load_model_stats()
    example_recommendations = load_example_recommendations()
    
    # Section d'introduction
    st.markdown("""
    Le recommandeur basé sur le contenu utilise les caractéristiques nutritionnelles et les types d'aliments
    pour générer des recommandations personnalisées. Il analyse le profil nutritionnel des aliments que vous
    aimez pour suggérer des aliments similaires.
    """)
    
    # Métriques principales
    if model_stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "MAE",
                f"{model_stats['model_metrics']['content_based']['mae']:.3f}",
                help="Erreur absolue moyenne entre les prédictions et les vraies préférences"
            )
        
        with col2:
            st.metric(
                "RMSE",
                f"{model_stats['model_metrics']['content_based']['rmse']:.3f}",
                help="Racine de l'erreur quadratique moyenne"
            )
        
        with col3:
            st.metric(
                "Couverture",
                f"{model_stats['model_metrics']['content_based']['coverage']:.1f}%",
                help="Pourcentage d'aliments que le modèle peut recommander"
            )
        
        with col4:
            st.metric(
                "Utilisateurs",
                f"{model_stats['general_statistics']['total_users']:,}",
                help="Nombre total d'utilisateurs dans le système"
            )
    
    # Visualisations
    st.header("Analyse des Données")
    
    if food_features is not None:
        tab1, tab2, tab3 = st.tabs([
            "Distribution des Caractéristiques",
            "Types d'Aliments",
            "Corrélations"
        ])
        
        with tab1:
            st.plotly_chart(
                plot_feature_distributions(food_features),
                use_container_width=True
            )
            
        with tab2:
            st.plotly_chart(
                plot_food_type_distribution(food_features),
                use_container_width=True
            )
            
        with tab3:
            st.plotly_chart(
                plot_feature_correlations(food_features),
                use_container_width=True
            )
    
    # Exemples de recommandations
    st.header("Exemples de Recommandations")
    
    if example_recommendations:
        # Sélection de l'utilisateur
        user_ids = list(example_recommendations.keys())
        selected_user = st.selectbox(
            "Sélectionner un utilisateur",
            user_ids,
            format_func=lambda x: f"Utilisateur {x}"
        )
        
        if selected_user:
            recs = example_recommendations[selected_user]
            
            # Afficher les recommandations dans un tableau
            if recs:
                df_recs = pd.DataFrame(recs)
                df_recs['Similarité'] = df_recs['similarity'].apply(lambda x: f"{x:.2%}")
                
                st.dataframe(
                    df_recs[['Nom', 'Type', 'Similarité']],
                    use_container_width=True
                )
            else:
                st.info("Pas de recommandations disponibles pour cet utilisateur")
    
    # Section technique
    st.header("Détails Techniques")
    
    with st.expander("Comment fonctionne le recommandeur basé sur le contenu ?"):
        st.markdown("""
        Le recommandeur basé sur le contenu fonctionne en plusieurs étapes :
        
        1. **Préparation des données**
           - Extraction des caractéristiques nutritionnelles
           - Encodage des types d'aliments
           - Normalisation des valeurs numériques
        
        2. **Création des profils**
           - Calcul des vecteurs de caractéristiques pour chaque aliment
           - Création des profils utilisateurs basés sur leurs préférences
        
        3. **Génération des recommandations**
           - Calcul de la similarité entre le profil utilisateur et les aliments
           - Sélection des aliments les plus similaires
           - Filtrage des aliments déjà consommés
        
        4. **Évaluation**
           - Calcul des métriques de performance (MAE, RMSE)
           - Analyse de la couverture du système
           - Validation sur un ensemble de test
        """)
    
    # Footer avec timestamp
    st.markdown("---")
    if model_stats:
        st.caption(
            f"Dernière mise à jour du modèle : "
            f"{datetime.fromisoformat(model_stats['general_statistics']['timestamp']).strftime('%d/%m/%Y %H:%M')}"
        )

if __name__ == "__main__":
    main()
