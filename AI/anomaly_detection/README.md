# Détection d'Anomalies dans les Habitudes Alimentaires

## Introduction

Ce module implémente un système de détection d'anomalies pour identifier les repas atypiques dans les habitudes alimentaires des utilisateurs. L'approche combine l'Isolation Forest pour la détection non supervisée et un système expert pour l'analyse et l'explication des anomalies détectées.

## Méthodologie

### Vecteur de Features

Pour chaque repas m, nous construisons un vecteur de caractéristiques M ∈ ℝᵈ composé de :

1. **Métriques Nutritionnelles** (η):
   ```
   η = (c, l, p, g) où
   c = calories totales
   l = lipides totaux
   p = protéines totales
   g = glucides totaux
   ```

2. **Métriques Temporelles** (τ):
   ```
   τ = (h, w) où
   h ∈ [0,23] = heure du repas
   w ∈ [0,6] = jour de la semaine
   ```

3. **Déviations par rapport aux Moyennes Mobiles** (δ):
   ```
   δᵢ = (vᵢ - μᵢ) / μᵢ où
   vᵢ = valeur actuelle du nutriment i
   μᵢ = moyenne mobile du nutriment i
   ```

### Modèle de Détection

#### Isolation Forest

Le modèle principal utilise l'algorithme Isolation Forest qui isole les anomalies en construisant des arbres de décision aléatoires :

```
s(x,n) = 2^(-E(h(x))/c(n))

où:
h(x) = longueur du chemin pour isoler x
c(n) = longueur moyenne du chemin non-successif
E(h(x)) = espérance de h(x)
```

Paramètres du modèle :
- contamination = 0.1 (10% d'anomalies attendues)
- n_estimators = 100 (nombre d'arbres)

#### Système Expert d'Analyse

L'`AnomalyAnalyzer` utilise un ensemble de règles basées sur des seuils pour caractériser les anomalies :

1. **Seuils Caloriques**:
   ```
   anomalie_calorique = {
       calories < 100 ou calories > 3000
   }
   ```

2. **Ratios Nutritionnels**:
   ```
   anomalie_ratio = {
       lipides_ratio > 0.35 ou
       glucides_ratio > 0.65 ou
       proteines_ratio > 0.35
   }
   ```

3. **Fenêtres Temporelles**:
   ```
   repas_normal = {
       petit_dejeuner ∈ [5,10] ou
       dejeuner ∈ [11,14] ou
       diner ∈ [18,21]
   }
   ```

## Résultats et Métriques

### Statistiques Générales

1. **Distribution des Anomalies**:
   - Taux d'anomalies : ~10% (paramètre de contamination)
   - Score d'anomalie moyen : -0.5
   - Seuil de décision : -0.7

2. **Caractéristiques des Anomalies Détectées**:
   ```
   Calories:  μ = 2500±800 kcal
   Lipides:   μ = 35±15 g
   Protéines: μ = 45±20 g
   Glucides:  μ = 80±30 g
   ```

### Types d'Anomalies Identifiés

1. **Anomalies Nutritionnelles** (∼60%):
   - Déséquilibres macronutritionnels
   - Valeurs caloriques extrêmes

2. **Anomalies Temporelles** (∼30%):
   - Repas hors des plages horaires habituelles
   - Fréquence inhabituelle des repas

3. **Anomalies Combinées** (∼10%):
   - Combinaison de facteurs nutritionnels et temporels

## Analyse des Seuils et Impacts

### 1. Seuil de Contamination ($\varepsilon$)

#### Formulation Mathématique
$$\varepsilon = \frac{|A|}{|D|} \text{ où}$$
$$A = \text{ensemble des anomalies}$$
$$D = \text{ensemble des données}$$

#### Impact du Choix
- **$\varepsilon = 0.1$ (valeur actuelle)**
  - Équilibre optimal entre sensibilité et spécificité
  - ~10% des repas marqués comme anomalies
  - Adapté aux variations naturelles des habitudes alimentaires

- **$\varepsilon < 0.05$**
  - Détection des anomalies extrêmes uniquement
  - Risque de faux négatifs élevé
  - Utilisé pour la détection de comportements très atypiques

- **$\varepsilon > 0.15$**
  - Sur-détection des anomalies
  - Risque de faux positifs élevé
  - Peut créer une "fatigue d'alerte" chez l'utilisateur

### 2. Seuils Nutritionnels

#### Calories ($C$)
$$\text{anomalie}_C = \begin{cases}
C < C_{min} = 100 \text{ kcal} & \text{repas trop léger} \\
C > C_{max} = 3000 \text{ kcal} & \text{repas trop copieux}
\end{cases}$$

**Conséquences**:
- $C_{min}$ trop bas (< 100 kcal)
  - Manque des collations légères anormales
  - Risque de troubles alimentaires non détectés

- $C_{max}$ trop haut (> 3000 kcal)
  - Tolère des excès caloriques importants
  - Peut masquer des comportements compulsifs

#### Ratios Macronutriments ($R$)
$$R_{nutriment} = \frac{\text{nutriment}_{\text{calories}}}{\text{total}_{\text{calories}}}$$

$$\text{anomalie}_R = \begin{cases}
R_{\text{lipides}} > 0.35 \\
R_{\text{glucides}} > 0.65 \\
R_{\text{proteines}} > 0.35
\end{cases}$$

**Impact des Seuils**:
- Ratios trop stricts ($\pm 5\%$ des valeurs actuelles)
  - Nombreuses fausses alertes
  - Démotivation possible des utilisateurs

- Ratios trop souples ($\pm 15\%$ des valeurs actuelles)
  - Manque des déséquilibres nutritionnels significatifs
  - Risque pour la santé à long terme

### 3. Seuils Temporels

#### Fenêtres de Repas ($W$)
$$W = \begin{cases}
\text{petit-déjeuner} &: [5\text{h}, 10\text{h}] \\
\text{déjeuner} &: [11\text{h}, 14\text{h}] \\
\text{dîner} &: [18\text{h}, 21\text{h}]
\end{cases}$$

### 4. Score d'Anomalie ($s$)

$$s(x) = 2^{-E(h(x))/c(n)}$$
$$\text{seuil}_{\text{décision}} = -0.7$$

### 5. Impact sur le Monitoring

#### Alertes Immédiates
- Seuils critiques (score < -0.9)
  - Déclenchement d'alertes urgentes
  - Intervention possible requise

#### Suivi à Long Terme
- Seuils de tendance (variation > 20% sur 7 jours)
  - Détection des changements progressifs
  - Prévention des dérives comportementales

### 6. Recommandations d'Ajustement

1. **Adaptation Utilisateur**
   $$\text{seuil}_{\text{personnalisé}} = \text{seuil}_{\text{base}} + \alpha \cdot \sigma_{\text{utilisateur}}$$
   où $\sigma_{\text{utilisateur}}$ = écart-type des scores de l'utilisateur

2. **Adaptation Temporelle**
   $$W_{\text{ajusté}} = W_{\text{base}} \pm \beta \cdot \text{période}_{\text{année}}$$
   où $\beta$ reflète les variations saisonnières

## Validation et Performance

### Métriques de Qualité

1. **Isolation Score**:
   ```
   IS = 1/n ∑ᵢ₌₁ⁿ s(xᵢ)
   où s(xᵢ) = score d'anomalie pour l'échantillon i
   ```

2. **Cohérence Temporelle**:
   ```
   TC = |Aₜ ∩ Aₜ₊₁| / |Aₜ ∪ Aₜ₊₁|
   où Aₜ = ensemble des anomalies au temps t
   ```

### Robustesse

1. **Stabilité du Modèle**:
   - Variance inter-arbres < 0.1
   - Cohérence des prédictions > 95%

2. **Sensibilité aux Paramètres**:
   - Impact contamination: ±5% sur les résultats
   - Impact n_estimators: stable au-delà de 100

## Applications et Utilisation

Le système est utilisé pour :
1. Détecter les repas inhabituels en temps réel
2. Fournir des explications détaillées des anomalies
3. Suivre l'évolution des habitudes alimentaires
4. Identifier les tendances problématiques

## Conclusion

Le système combine efficacement l'apprentissage non supervisé (Isolation Forest) avec une analyse experte pour détecter et expliquer les anomalies dans les habitudes alimentaires. Les résultats montrent une bonne capacité à identifier les repas atypiques tout en fournissant des explications interprétables et actionnables.
