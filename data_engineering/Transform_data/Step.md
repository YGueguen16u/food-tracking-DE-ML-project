
### 1. Lire les données stockées dans `data/raw`

- **Consigne pour ton projet** :
  Écrire un script en Python qui lit les fichiers CSV stockés dans le dossier `data/raw`. Ces fichiers contiennent des enregistrements de repas. Le script doit lire tous les fichiers, les concaténer en une seule DataFrame, et afficher un aperçu de la DataFrame consolidée.

- **Complexité supplémentaire** :
  - Ajoute une étape qui vérifie la cohérence des colonnes entre les fichiers et qui lève une alerte si des colonnes sont manquantes ou si des colonnes inattendues apparaissent.
  - Ajoute une analyse des individus : compte le nombre d’utilisateurs uniques (`user_id`) et le nombre moyen de repas par utilisateur.

### 2. Regrouper la donnée par jour

- **Consigne pour ton projet** :
  Faire un GroupBy sur la colonne `date` et agréger les calories (`total_calories`), lipides (`total_lipids`), glucides (`total_carbs`) et protéines (`total_protein`) consommés par jour. Afficher ensuite le résultat de l’agrégation.
  Effectuer un GroupBy sur les colonnes `date` et `user_id` pour obtenir le total journalier des calories, lipides, glucides et protéines consommés par utilisateur.

- **Complexité supplémentaire** :
  - Ajoute une agrégation supplémentaire pour calculer la consommation moyenne par aliment par jour (par exemple, la moyenne des calories pour chaque aliment consommé chaque jour).
  - Crée une visualisation pour montrer les variations des nutriments consommés (calories, lipides, etc.) au fil du temps.
  - Crée un groupement supplémentaire pour analyser la répartition des nutriments par utilisateur et par type d’aliment.
  - Crée un groupement supplémentaire pour analyser la répartition des nutriments par utilisateur et par type d’aliment.

### 3. Filtrer les nulls et les valeurs inattendues

- **Consigne pour ton projet** :
  Supprimer les enregistrements qui ont des valeurs nulles ou qui contiennent des données inattendues, comme des unités aberrantes (par exemple, une quantité en litres ou en kWh là où tu attends des grammes ou des calories).

- **Complexité supplémentaire** :
  - Implémente un contrôle pour les valeurs extrêmes (par exemple, des repas avec des valeurs de calories beaucoup trop élevées ou trop basses) et marque ces lignes pour un examen ultérieur sans les supprimer.
  - Analyse la répartition des valeurs nulles ou aberrantes par utilisateur pour identifier des utilisateurs avec des données particulièrement problématiques.
  - Conserve un journal des lignes supprimées, ce qui te permettra d’examiner les données problématiques ultérieurement.
  - Ajoute une étape de vérification des doublons après avoir supprimé les lignes incorrectes.

### 4. Faire une Window Function

- **Consigne pour ton projet** :
  Implémenter une fonction de fenêtrage (`window function`) qui calcule la moyenne des nutriments (calories, lipides, glucides, protéines) sur les quatre derniers enregistrements de repas pour le même jour de la semaine (par exemple, les quatre derniers lundis pour un lundi donné).
  Implémenter une fonction de fenêtrage qui calcule la moyenne des nutriments sur les quatre derniers jours similaires de la semaine pour chaque utilisateur (par exemple, les quatre derniers lundis pour un lundi donné).

- **Complexité supplémentaire** :
  - Ajouter une colonne qui compare les valeurs actuelles de nutriments avec la moyenne des quatre dernières valeurs pour le même jour de la semaine, pour identifier des tendances alimentaires spécifiques.
  - Implémente un mécanisme pour identifier les jours où la consommation dévie fortement de cette moyenne (par exemple, en utilisant des écarts-types).
  - Comparer les habitudes alimentaires de chaque utilisateur à la moyenne des autres utilisateurs similaires (par exemple, même tranche d'âge, même sexe).
  - Créer une segmentation des utilisateurs en fonction de leur régularité alimentaire (par exemple, utilisateurs avec des apports constants versus utilisateurs avec des fluctuations importantes).

### 5. Créer une colonne "percentage_change"

- **Consigne pour ton projet** :
  Créer une nouvelle colonne `pct_change` qui mesure le pourcentage d’écart entre la valeur actuelle d’un nutriment (calories, lipides, glucides, protéines) et la moyenne des quatre derniers jours similaires de la semaine (calculée à l’étape précédente).
  Créer une colonne `pct_change` qui mesure l'écart en pourcentage entre la valeur actuelle d’un nutriment pour un utilisateur et la moyenne des quatre derniers jours similaires de la semaine.

- **Complexité supplémentaire** :
  - Calcule le pourcentage de changement pour chaque nutriment (calories, lipides, glucides, protéines) individuellement et ajoute une colonne pour chaque mesure.
  - Crée une alerte (marquage) pour les repas où les changements de pourcentage dépassent un certain seuil (par exemple, +/- 20%).
  - Analyser les changements de pourcentage par utilisateur pour identifier des tendances individuelles (par exemple, quelqu'un qui mange plus les week-ends ou qui a des variations saisonnières).
  - Créer des indicateurs de performance (KPI) qui comparent les habitudes alimentaires des utilisateurs entre eux.

### 6. Stocker la donnée au format Parquet

- **Consigne pour ton projet** :
  Après avoir filtré et transformé les données, stocker le résultat final dans un fichier Parquet dans le dossier `data/processed`. Ce format est efficace en termes de stockage et de vitesse de lecture, ce qui est idéal pour des analyses futures.
  Stocker les données transformées au format Parquet, en séparant les fichiers par utilisateur dans le dossier `data/processed`. Cela permettra des analyses plus ciblées à l’avenir.

- **Complexité supplémentaire** :
  - Divise les données transformées en plusieurs fichiers Parquet basés sur les catégories de nutriments (par exemple, un fichier pour les calories, un autre pour les lipides, etc.).
  - Implémente une compression (par exemple, `snappy` ou `gzip`) pour les fichiers Parquet pour réduire l'espace disque utilisé.
  - Implémenter une structure de dossiers hiérarchiques basée sur les utilisateurs pour faciliter la gestion des fichiers Parquet.
  - Ajouter une analyse des tailles de fichier pour optimiser le stockage des données.
