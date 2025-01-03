# Système de Recommandation Alimentaire

## Introduction

Ce module implémente un système de recommandation hybride combinant le filtrage collaboratif et le filtrage basé sur le contenu pour suggérer des repas adaptés aux utilisateurs. Le système prend en compte les habitudes alimentaires, les préférences nutritionnelles et les similarités entre utilisateurs.

## Méthodologie

### Composantes du Système

1. **Filtrage Collaboratif (User-User)**
   - Matrice utilisateur-repas
   - Similarité cosinus entre utilisateurs
   - Prédiction des notes basée sur le voisinage

2. **Filtrage Basé sur le Contenu**
   - Profils nutritionnels
   - Préférences temporelles
   - Restrictions alimentaires

3. **Hybridation**
   - Combinaison pondérée des recommandations
   - Ajustement contextuel (heure, jour)
   - Diversification des suggestions

### Features Utilisées

1. **Profil Utilisateur** ($U$):
   ```
   U = (P, T, R) où:
   P = profil nutritionnel préféré
   T = distribution temporelle des repas
   R = historique des notes
   ```

2. **Profil Repas** ($M$):
   ```
   M = (N, C, F) où:
   N = métriques nutritionnelles
   C = catégorie du repas
   F = fréquence de consommation
   ```

### Algorithmes

#### Filtrage Collaboratif
```
sim(u,v) = cos(R_u, R_v)
pred(u,i) = μ_u + Σ(sim(u,v) * (r_vi - μ_v)) / Σ|sim(u,v)|
```

#### Filtrage Basé sur le Contenu
```
sim(m1,m2) = cos(M_1, M_2)
score(u,m) = Σ(w_i * sim(m, m_i)) pour m_i ∈ historique_u
```

## Métriques d'Évaluation

1. **Précision et Rappel**
   - Précision@k
   - Rappel@k
   - MAP (Mean Average Precision)

2. **Diversité**
   - Intra-list Distance
   - Coverage des catégories

3. **Personnalisation**
   - User Space Coverage
   - Tail Item Coverage
