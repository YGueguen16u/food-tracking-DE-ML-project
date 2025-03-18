# Structure des DataFrames dans S3

Ce document répertorie tous les DataFrames du bucket S3 et leurs colonnes respectives.

## reference_data/food
### food_processed.xlsx
Colonnes :
- id
- Aliment
- Type
- quantitee
- unit_quantity
- Valeur calorique
- unit_cal
- Lipides
- unit_lip
- Graisses saturee
- unit_saturee
- Graisses mono-insaturee
- unit_mono_ins
- Graisses polyinsaturee
- unit_poly_ins
- Glucides
- unit_glu
- Sucre
- unit_sucre
- Protein
- unit_prot
- Fibre alimentaire
- unit_fibre
- Cholesterol
- unit_chol
- Sodium
- unit_sodium
- Eau
- unit_eau
- Vitamine A
- unit_vitA
- Vitamine B1
- unit_vitB1
- Vitamine B11
- unit_vitB11
- Vitamine B12
- unit_vitB12
- Vitamine B2
- unit_vitB2
- Vitamine B3
- unit_vitB3
- Vitamine B5
- unit_vitB5
- Vitamine B6
- unit_vitB6
- Vitamine C
- unit_vitC
- Vitamine D
- unit_vitD
- Vitamine E
- unit_vitE
- Vitamine K
- unit_vitK
- Calcium
- unit_calcium
- Cuivre
- unit_cuivre
- Fer
- unit_fer
- Magnesium
- unit_magnesium
- Manganese
- unit_manganese
- Phosphore
- unit_phosphore
- Potassium
- unit_potassium
- Selenium
- unit_selenium
- Zinc
- unit_zinc

### type_food.xlsx
Colonnes :
- Unnamed: 0
- 0

## transform/folder_1_combine
### combined_meal_data.xlsx
Colonnes :
- meal_record_id
- user_id
- meal_id
- date
- heure
- aliment_id
- quantity
- Aliment
- Valeur calorique
- Lipides
- Glucides
- Protein
- total_calories
- total_lipids
- total_carbs
- total_protein

## transform/folder_2_filter_data
### combined_meal_data_filtered.xlsx
Colonnes :
- meal_record_id
- user_id
- meal_id
- date
- heure
- aliment_id
- quantity
- Aliment
- Valeur calorique
- Lipides
- Glucides
- Protein
- total_calories
- total_lipids
- total_carbs
- total_protein
- quantity_status

## transform/folder_3_group_data
### duckdb_aggregation_results.xlsx
Colonnes :
- date
- total_calories
- total_lipids
- total_carbs
- total_protein
- distinct_user_count

### pandas_aggregation_results.xlsx
Colonnes :
- date
- total_calories
- total_lipids
- total_carbs
- total_protein
- distinct_user_count

## transform/folder_4_windows_function/daily
### daily_window_function_duckdb.xlsx
Colonnes :
- date
- total_calories
- total_lipids
- total_carbs
- total_protein
- weekday
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein

### daily_window_function_pandas.xlsx
Colonnes :
- date
- total_calories
- total_lipids
- total_carbs
- total_protein
- distinct_user_count
- weekday
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein

## transform/folder_4_windows_function/type_food
### user_food_proportion_duckdb.xlsx
Colonnes :
- user_id
- Type
- total_calories
- total_lipids
- total_protein
- total_carbs
- proportion_total_calories
- proportion_total_lipids
- proportion_total_protein
- proportion_total_carbs

### user_food_proportion_pandas.xlsx
Colonnes :
- user_id
- Type
- total_calories
- total_lipids
- total_carbs
- total_protein
- food_count
- avg_calories_per_type
- avg_lipids_per_type
- avg_carbs_per_type
- avg_protein_per_type
- proportion_total_calories
- proportion_total_lipids
- proportion_total_protein
- proportion_total_carbs

## transform/folder_4_windows_function/user
### user_daily_window_function_duckdb.xlsx
Colonnes :
- date
- user_id
- total_calories
- total_lipids
- total_carbs
- total_protein
- rank
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein

### user_daily_window_function_pandas.xlsx
Colonnes :
- date
- user_id
- total_calories
- total_lipids
- total_carbs
- total_protein
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein

## transform/folder_5_percentage_change/daily
### daily_percentage_change_duckdb.xlsx
Colonnes :
- date
- total_calories
- total_lipids
- total_carbs
- total_protein
- weekday
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein
- percentage_change_total_calories
- percentage_change_total_lipids
- percentage_change_total_carbs
- percentage_change_total_protein

### daily_percentage_change_pandas.xlsx
Colonnes :
- date
- total_calories
- total_lipids
- total_carbs
- total_protein
- distinct_user_count
- weekday
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein
- percentage_change_total_calories
- percentage_change_total_lipids
- percentage_change_total_carbs
- percentage_change_total_protein

## transform/folder_5_percentage_change/user
### user_daily_percentage_change_duckdb.xlsx
Colonnes :
- date
- user_id
- total_calories
- total_lipids
- total_carbs
- total_protein
- rank
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein
- rank_1
- rolling_avg_total_calories_1
- rolling_avg_total_lipids_1
- rolling_avg_total_carbs_1
- rolling_avg_total_protein_1
- percentage_change_total_calories
- percentage_change_total_lipids
- percentage_change_total_carbs
- percentage_change_total_protein

### user_daily_percentage_change_pandas.xlsx
Colonnes :
- date
- user_id
- total_calories
- total_lipids
- total_carbs
- total_protein
- rolling_avg_total_calories
- rolling_avg_total_lipids
- rolling_avg_total_carbs
- rolling_avg_total_protein
- percentage_change_total_calories
- percentage_change_total_lipids
- percentage_change_total_carbs
- percentage_change_total_protein

