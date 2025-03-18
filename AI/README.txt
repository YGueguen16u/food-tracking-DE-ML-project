Structure des Algorithmes d'IA pour le Food Tracking

1. Structure des Dossiers
/AI
├── time_series/               # Prédictions temporelles
│   ├── models/               # Modèles entraînés
│   ├── training/            # Scripts d'entraînement
│   └── inference/           # Scripts de prédiction
│
├── recommender/              # Système de recommandation
│   ├── content_based/       # Recommandations basées sur le contenu
│   ├── collaborative/       # Filtrage collaboratif
│   └── hybrid/             # Système hybride
│
├── anomaly_detection/        # Détection d'anomalies
│   ├── models/             
│   └── rules/              # Règles métier pour les anomalies
│
├── clustering/               # Clustering des habitudes
│   ├── models/
│   └── analysis/           # Scripts d'analyse des clusters
│
├── utils/                    # Utilitaires communs
│   ├── data_prep/          # Préparation des données
│   ├── evaluation/         # Métriques d'évaluation
│   └── visualization/      # Visualisation des résultats
│
└── config/                   # Configuration des modèles

2. Description des Algorithmes

a) Time Series (Prédictions Temporelles)
- Objectif : Prédire les calories journalières futures
- Modèles : Prophet, SARIMA
- Features : Historique calories, jour semaine
- Métriques : MAE, RMSE, MAPE

b) Recommender System
- Objectif : Suggérer des aliments pertinents
- Approches : 
  * Content-based : basé sur les nutriments et préférences
  * Collaborative : basé sur les comportements similaires
- Métriques : Precision@K, Recall@K, NDCG

c) Anomaly Detection
- Objectif : Identifier les repas inhabituels
- Modèles : Isolation Forest, règles statistiques
- Features : Calories, macronutriments, timing
- Métriques : Precision, Recall, F1-Score

d) Clustering
- Objectif : Grouper les utilisateurs par habitudes
- Modèles : K-Means, DBSCAN
- Features : Moyennes calories, timing repas, préférences
- Métriques : Silhouette Score, Calinski-Harabasz

3. Pipeline de Données
- Source : S3 (transform/folder_1_combine/combined_meal_data.xlsx)
- Preprocessing : Nettoyage, normalisation, feature engineering
- Train/Test Split : 80/20 avec validation temporelle
- Sauvegarde : Modèles dans S3 (ai_models/)

4. Prochaines Étapes
1. Implémenter le preprocessing des données
2. Développer les modèles un par un
3. Créer une API pour les prédictions
4. Intégrer avec le frontend

5. Notes Importantes
- Utiliser scikit-learn pour la majorité des modèles
- Sauvegarder les métriques pour le suivi
- Documenter les choix d'hyperparamètres
- Prévoir des tests unitaires
- Mettre en place un monitoring des performances
