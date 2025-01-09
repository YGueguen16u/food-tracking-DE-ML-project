# Content-Based Food Recommender System

## Résultats des Tests de Performance

Le modèle a été évalué sur un jeu de données comprenant :
- 19 utilisateurs
- 629 aliments uniques
- 9,259 interactions totales
- Division : 80% entraînement (7,416 interactions) / 20% test (1,843 interactions)

### Métriques de Performance :

| Métrique | Score | Description | Interprétation |
|----------|--------|-------------|----------------|
| MAE | 1.758 | Erreur moyenne absolue entre les prédictions et les vraies notes | **Modéré** - En moyenne, les prédictions dévient de 1.758 points sur une échelle de 0-5. Une erreur de cette amplitude suggère que le modèle capture les tendances générales mais manque de précision fine. |
| RMSE | 1.999 | Racine carrée de l'erreur quadratique moyenne | **Modéré** - La présence d'erreurs importantes est indiquée par un RMSE proche de 2. L'écart avec le MAE (0.241) suggère une variabilité significative dans la précision des prédictions. |
| Couverture | 100% | Pourcentage d'aliments que le système peut recommander | **Excellent** - Le modèle peut générer des recommandations pour tous les aliments du catalogue, assurant une diversité maximale dans les suggestions. |

### Points Forts :
- ✅ Couverture parfaite du catalogue
- ✅ Pas de démarrage à froid pour les nouveaux aliments
- ✅ Recommandations explicables via les caractéristiques nutritionnelles
- ✅ Mise à jour instantanée des préférences utilisateur

### Points à Améliorer :
- ❌ Précision modérée des prédictions (MAE > 1.5)
- ❌ Présence d'erreurs importantes (RMSE ≈ 2)
- ❌ Forte dépendance à la qualité des données nutritionnelles

## Description Détaillée du Système

## Description
Système de recommandation basé sur le contenu qui utilise les caractéristiques nutritionnelles des aliments pour générer des recommandations personnalisées. Le système analyse les préférences passées des utilisateurs et trouve des aliments similaires en termes de profil nutritionnel.

## Caractéristiques Principales
- Recommandations basées sur 7 caractéristiques nutritionnelles
- Profils utilisateurs dynamiques
- Similarité cosinus pour le matching
- Couverture complète du catalogue d'aliments

## Features Utilisées
- Calories (0-942 kcal)
- Lipides (0-343.4g)
- Glucides
- Protéines
- Fibres
- Sucres (0-143g)
- Sodium (0-5.6g)

## Architecture

### Composants Principaux
1. **DataLoader** (`data_loader.py`)
   - Chargement des données depuis S3
   - Prétraitement des features nutritionnelles
   - Nettoyage et validation des données

2. **ContentBasedRecommender** (`food_recommender.py`)
   - Construction des profils utilisateurs
   - Calcul des similarités cosinus
   - Génération des recommandations
   - Évaluation des performances

3. **Training Pipeline** (`training/train_model.py`)
   - Division train/test
   - Entraînement du modèle
   - Calcul des métriques
   - Sauvegarde des résultats dans S3

## Processus de Recommandation

1. **Construction du Profil Utilisateur**
   ```python
   user_profile = moyenne_pondérée(aliments_consommés, ratings)
   ```

2. **Calcul des Similarités**
   ```python
   similarités = cosine_similarity(user_profile, food_vectors)
   ```

3. **Génération des Recommandations**
   ```python
   recommendations = top_k(similarités * 5, k=10)
   ```

## Avantages et Limitations

### Avantages
- ✅ Pas de démarrage à froid pour les nouveaux aliments
- ✅ Recommandations explicables
- ✅ Couverture complète du catalogue
- ✅ Mise à jour instantanée des préférences

### Limitations
- ❌ MAE et RMSE modérés
- ❌ Nécessite des données nutritionnelles complètes
- ❌ Sensible à la qualité des features

## Pistes d'Amélioration

1. **Prétraitement des Features**
   - Normalisation des valeurs nutritionnelles
   - Feature scaling adaptatif
   - Réduction de dimensionnalité

2. **Calcul des Similarités**
   - Pondération des features nutritionnelles
   - Métriques de distance alternatives
   - Calibration des scores

3. **Features Supplémentaires**
   - Types d'aliments
   - Vitamines et minéraux
   - Contexte temporel

## Utilisation

### Installation
```bash
pip install -r requirements.txt
```

### Entraînement
```bash
python -m AI.recommender.content_based.training.train_model
```

### Génération de Recommandations
```python
recommender = ContentBasedRecommender()
recommendations = recommender.recommend(user_id)
```

## Intégration

Le système est intégré dans l'application Streamlit via :
- Page dédiée aux recommandations
- Visualisations des métriques
- Interface de test des recommandations
