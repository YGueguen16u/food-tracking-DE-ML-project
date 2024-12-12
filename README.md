# Food Tracking Data Engineering Project

Ce projet est une solution complète de traitement et d'analyse de données pour le suivi alimentaire. Il comprend plusieurs composants pour l'extraction, la transformation et l'analyse des données nutritionnelles.

## Structure du Projet

```
data_engineering/
├── Extract_data/         # Scripts pour l'extraction des données
├── Transform_data/       # Scripts de transformation des données
├── Webscrapping/        # Scripts de web scraping
├── test/                # Tests unitaires
└── sensor_api/          # API pour les capteurs
```

## Composants Principaux

### 1. Transformation des Données (Transform_data)

Le module de transformation des données comprend plusieurs étapes clés :

#### 1.1 Agrégation des Données
- Combine les données de repas de différentes sources
- Filtre et nettoie les données selon des critères spécifiques
- Groupe les données par différentes dimensions (journalier, utilisateur, type d'aliment)

#### 1.2 Analyses Statistiques
- Calcul des moyennes mobiles sur les nutriments
- Analyse des variations en pourcentage
- Comparaison des tendances quotidiennes et hebdomadaires

#### 1.3 Technologies Utilisées
- **Pandas** : Pour le traitement des données tabulaires
- **DuckDB** : Pour les requêtes SQL performantes sur les données
- **Excel** : Pour le stockage et la visualisation des résultats

### 2. Tests (test/)

Le projet inclut une suite de tests complète pour assurer la qualité et la fiabilité du code :
- Tests unitaires pour chaque composant
- Tests d'intégration pour les flux de données
- Validation des résultats de transformation

## Installation

1. Cloner le repository :
```bash
git clone [URL_du_repo]
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

## Utilisation

### Transformation des Données

Pour exécuter la transformation complète des données :

1. Placer les données brutes dans le dossier `data/raw/`
2. Exécuter les scripts de transformation dans l'ordre :
```python
python data_engineering/Transform_data/combine_meal.py
python data_engineering/Transform_data/filter_data.py
python data_engineering/Transform_data/group_data.py
```

### Tests

Pour exécuter la suite de tests :
```bash
pytest data_engineering/test/
```

## Contribution

Les contributions sont les bienvenues ! Veuillez suivre ces étapes :
1. Fork le projet
2. Créer une branche pour votre fonctionnalité
3. Commiter vos changements
4. Pousser vers la branche
5. Ouvrir une Pull Request

## Licence

[Type de licence]

## Contact

[Vos informations de contact]
