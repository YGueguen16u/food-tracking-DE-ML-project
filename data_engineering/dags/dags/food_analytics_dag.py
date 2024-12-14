from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin du projet au PYTHONPATH
project_dir = '/opt/airflow/project'
sys.path.append(project_dir)

# Import des scripts de transformation
from data_engineering.Extract_data.smart_extract import main as smart_extract
from data_engineering.Transform_data.transform_1_read_data import main as read_data
from data_engineering.Transform_data.transform_1_combine_meal import main as combine_meal
from data_engineering.Transform_data.transform_2_filter_data import main as filter_data
from data_engineering.Transform_data.transform_3_group_data import main as group_data
from data_engineering.Transform_data.transform_4_1_window_function_daily import main as window_daily
from data_engineering.Transform_data.transform_4_2_windows_function_user import main as window_user
from data_engineering.Transform_data.transform_4_3_window_function_type_food import main as window_type_food
from data_engineering.Transform_data.transform_5_1_percentage_change_daily import main as percentage_daily
from data_engineering.Transform_data.transform_5_2_percentage_change_user import main as percentage_user
from data_engineering.Transform_data.transform_5_3_percentage_change_type_food import main as percentage_type_food
from data_engineering.Transform_data.transform_6_parquet import main as parquet_conversion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'food_analytics_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour Food Analytics',
    schedule_interval=timedelta(days=1),
)

# Tâche d'extraction
extract_task = PythonOperator(
    task_id='smart_extract',
    python_callable=smart_extract,
    dag=dag,
)

# Tâches de lecture et combinaison
read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    dag=dag,
)

combine_meal_task = PythonOperator(
    task_id='combine_meal',
    python_callable=combine_meal,
    dag=dag,
)

# Tâches de filtrage et groupement
filter_data_task = PythonOperator(
    task_id='filter_data',
    python_callable=filter_data,
    dag=dag,
)

group_data_task = PythonOperator(
    task_id='group_data',
    python_callable=group_data,
    dag=dag,
)

# Tâches de window functions
window_daily_task = PythonOperator(
    task_id='window_daily',
    python_callable=window_daily,
    dag=dag,
)

window_user_task = PythonOperator(
    task_id='window_user',
    python_callable=window_user,
    dag=dag,
)

window_type_food_task = PythonOperator(
    task_id='window_type_food',
    python_callable=window_type_food,
    dag=dag,
)

# Tâches de percentage change
percentage_daily_task = PythonOperator(
    task_id='percentage_daily',
    python_callable=percentage_daily,
    dag=dag,
)

percentage_user_task = PythonOperator(
    task_id='percentage_user',
    python_callable=percentage_user,
    dag=dag,
)

percentage_type_food_task = PythonOperator(
    task_id='percentage_type_food',
    python_callable=percentage_type_food,
    dag=dag,
)

# Tâche de conversion en parquet
parquet_conversion_task = PythonOperator(
    task_id='parquet_conversion',
    python_callable=parquet_conversion,
    dag=dag,
)

# Définition des dépendances dans l'ordre
extract_task >> read_data_task >> combine_meal_task >> filter_data_task >> group_data_task

# Window functions en parallèle
group_data_task >> [window_daily_task, window_user_task, window_type_food_task]

# Percentage changes en parallèle après leurs window functions respectives
window_daily_task >> percentage_daily_task
window_user_task >> percentage_user_task
window_type_food_task >> percentage_type_food_task

# Conversion finale en parquet après tous les calculs
[percentage_daily_task, percentage_user_task, percentage_type_food_task] >> parquet_conversion_task
