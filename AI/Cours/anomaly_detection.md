# Détection d'Anomalies avec Isolation Forest

## 1. Fondements Théoriques

### 1.1 Principe de Base
L'Isolation Forest est basé sur le principe que les anomalies sont :
- Plus rares que les instances normales
- Ont des valeurs significativement différentes des instances normales

La méthode exploite le fait que les anomalies sont plus facilement "isolables" dans l'espace des caractéristiques, nécessitant moins de partitions aléatoires pour être isolées du reste des données.

### 1.2 Fonctionnement Algorithmique

#### Construction de l'arbre d'isolation
1. Sélection aléatoire d'une caractéristique
2. Sélection aléatoire d'une valeur de split entre min et max de cette caractéristique
3. Partition récursive jusqu'à isolation complète ou atteinte de la limite de hauteur

#### Score d'Anomalie
Pour un point x, le score s(x) est calculé comme :
```
s(x) = 2^(-E[h(x)]/c(n))
```
Où :
- h(x) : longueur du chemin pour isoler x
- E[h(x)] : moyenne des longueurs sur tous les arbres
- c(n) : longueur moyenne du chemin infructueux dans un BST
- c(n) = 2H(n-1) - (2(n-1)/n), avec H(i) = ln(i) + 0.5772156649 (constante d'Euler)

## 2. Paramètres Clés

### 2.1 Contamination Factor (contamination)
- **Définition** : Proportion attendue d'anomalies dans le dataset
- **Impact** : Détermine le seuil de décision pour classifier une instance comme anomalie
- **Plage typique** : [0.01, 0.2]
- **Notre valeur** : 0.1 (10% d'anomalies attendues)

### 2.2 Nombre d'Arbres (n_estimators)
- **Définition** : Nombre d'arbres d'isolation dans la forêt
- **Impact** : 
  * Plus d'arbres = plus stable mais plus coûteux
  * Moins d'arbres = plus rapide mais plus variable
- **Plage typique** : [100, 1000]

### 2.3 Taille d'Échantillon (max_samples)
- **Définition** : Nombre d'échantillons pour entraîner chaque arbre
- **Impact** : Affecte la variance et la vitesse d'entraînement
- **Options** :
  * 'auto' : min(256, taille_dataset)
  * int : nombre exact d'échantillons
  * float : pourcentage du dataset

## 3. Métriques et Évaluation

### 3.1 Score d'Anomalie
- **Plage** : [-1, 1] théoriquement, mais souvent [-0.8, 0.5] en pratique
- **Interprétation** :
  * Proche de 1 : définitivement normal
  * Proche de 0 : indécis
  * Proche de -1 : définitivement anomalie

### 3.2 Métriques de Performance
1. **ROC-AUC Score**
   - Mesure la capacité à distinguer les classes
   - Insensible au déséquilibre des classes
   - Plage : [0, 1], >0.5 meilleur que aléatoire

2. **Precision-Recall AUC**
   - Plus approprié pour les datasets déséquilibrés
   - Met l'accent sur la détection correcte des anomalies

3. **Average Path Length**
   - Longueur moyenne du chemin pour isoler les points
   - Indicateur de la facilité d'isolation

## 4. Avantages et Limitations

### 4.1 Avantages
- Non paramétrique
- Ne fait pas d'hypothèse sur la distribution
- Efficace en haute dimension
- Rapide en entraînement et inférence
- Naturellement parallélisable

### 4.2 Limitations
- Sensible au bruit dans les données
- Peut être instable avec peu de données
- Pas optimal pour les anomalies très locales
- Difficulté avec les anomalies collectives

## 5. Optimisations Avancées

### 5.1 Extended Isolation Forest
- Utilise des hyperplans obliques au lieu d'axes parallèles
- Meilleure capture des relations entre variables
- Plus robuste aux rotations des données

### 5.2 Feature Bagging
- Sous-échantillonnage aléatoire des features
- Améliore la robustesse
- Réduit l'impact des features non pertinentes

### 5.3 Ensemble Methods
- Combinaison avec d'autres détecteurs
- Voting ou stacking
- Améliore la robustesse aux faux positifs

## 6. Application à Notre Cas

### 6.1 Configuration Actuelle
```python
{
    "contamination_factor": 0.1,
    "random_state": 42,
    "n_estimators": 100,  # valeur par défaut
    "max_samples": "auto"
}
```

### 6.2 Statistiques Observées
- Total échantillons : 20,042
- Anomalies détectées : 2,015 (10.05%)
- Score moyen : -0.457
- Plage de scores : [-0.737, -0.389]

### 6.3 Caractéristiques des Anomalies
Ratios moyens anomalies/normaux :
- Calories : 5.45x (448.06/82.15)
- Lipides : 24.03x (50.46/2.1)
- Glucides : 5.11x (63.62/12.44)
- Protéines : 11.21x (40.9/3.65)

## 7. Recommandations d'Optimisation

1. **Ajustement du Contamination Factor**
   - Basé sur l'expertise métier
   - Validation croisée pour optimisation

2. **Feature Engineering**
   - Ratios nutritionnels
   - Variables temporelles
   - Agrégations glissantes

3. **Ensemble Learning**
   - Combiner avec LOF ou DBSCAN
   - Weighted voting system
   - Meta-features extraction
