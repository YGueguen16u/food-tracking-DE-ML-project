# Préparation des Données pour les Modèles d'IA

## Sources de Données Requises

### 1. Données Transformées
- `transform_2_filter_data.py` : Version nettoyée des repas
- `transform_3_group_data.py` : Données agrégées
- `transform_4_*_window_function_*.py` : Features temporelles
- `transform_5_*_percentage_change_*.py` : Variations

### 2. Tables de Référence
- **Table Utilisateurs**
  - Colonnes : user_id, nom, sexe
  - Note : La colonne 'profil' sera omise pour les prédictions
- **Table Aliments**
  - Types d'aliments
  - Macronutriments
  - Catégories (sucreries, légumes, etc.)

## Utilisation par Modèle

### Time Series
- Sources principales :
  - `transform_4_1_window_function_daily.py`
  - `transform_3_group_data.py`
- Features créées :
  - Moyennes mobiles
  - Tendances quotidiennes
  - Saisonnalité hebdomadaire

### Recommender System
- Sources principales :
  - `transform_4_2_windows_function_user.py`
  - `transform_4_3_window_function_type_food.py`
  - Table aliments
- Features créées :
  - Profils utilisateurs
  - Préférences par type d'aliment
  - Matrices d'interaction

### Anomaly Detection
- Sources principales :
  - `transform_5_*_percentage_change_*.py`
  - `transform_4_1_window_function_daily.py`
- Features créées :
  - Variations significatives
  - Écarts à la moyenne
  - Patterns inhabituels

### Clustering
- Sources principales :
  - `transform_3_group_data.py`
  - `transform_4_2_windows_function_user.py`
- Features créées :
  - Habitudes alimentaires
  - Distribution temporelle des repas
  - Préférences nutritionnelles

## Notes Importantes
1. Toujours utiliser `load_and_merge_data()` pour fusionner correctement les sources
2. La colonne 'profil' de la table utilisateurs est exclue pour éviter le biais
3. Les données transformées doivent être complètement prêtes avant l'entraînement
4. Vérifier la cohérence des types de données après fusion
