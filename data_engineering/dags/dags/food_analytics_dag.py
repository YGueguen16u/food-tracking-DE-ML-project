from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from airflow.models import Variable
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)

# Configuration du logging
logger = logging.getLogger(__name__)

# Ajouter les chemins nécessaires au PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.append(project_dir)

# Fonction wrapper pour la gestion des erreurs
def task_wrapper(func):
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"Démarrage de la tâche {func.__name__}")
            result = func(*args, **kwargs)
            logger.info(f"Tâche {func.__name__} terminée avec succès")
            return result
        except Exception as e:
            logger.error(f"Erreur dans la tâche {func.__name__}: {str(e)}")
            raise
    return wrapper

# Import des scripts de transformation avec gestion d'erreurs
@task_wrapper
def import_and_run_smart_extract(**kwargs):
    logger.info("Démarrage de la tâche import_and_run_smart_extract")
    try:
        import sys
        import os
        # Add the project directory to sys.path
        project_root = '/opt/airflow/project'
        sys.path.append(project_root)
        
        from data_engineering.Extract_data.smart_extract import main
        main()
    except Exception as e:
        logger.error(f"Erreur dans la tâche import_and_run_smart_extract: {str(e)}")
        raise e

@task_wrapper
def import_and_run_read_data(**kwargs):
    logger.info("Démarrage de la tâche import_and_run_read_data")
    try:
        import sys
        import os
        # Add the project directory to sys.path
        project_root = '/opt/airflow/project'
        sys.path.append(project_root)
        
        from data_engineering.Transform_data.transform_1_read_data import main
        main()
    except Exception as e:
        logger.error(f"Erreur dans la tâche import_and_run_read_data: {str(e)}")
        raise e

@task_wrapper
def import_and_run_combine_meal(**kwargs):
    logger.info("Démarrage de la tâche import_and_run_combine_meal")
    try:
        import sys
        import os
        # Add the project directory to sys.path
        project_root = '/opt/airflow/project'
        sys.path.append(project_root)
        
        from data_engineering.Transform_data.transform_1_combine_meal import main
        main()
    except Exception as e:
        logger.error(f"Erreur dans la tâche import_and_run_combine_meal: {str(e)}")
        raise e

@task_wrapper
def import_and_run_filter_data(**kwargs):
    from data_engineering.Transform_data.transform_2_filter_data import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_group_data(**kwargs):
    from data_engineering.Transform_data.transform_3_group_data import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_window_daily(**kwargs):
    from data_engineering.Transform_data.transform_4_1_window_function_daily import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_window_user(**kwargs):
    from data_engineering.Transform_data.transform_4_2_windows_function_user import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_window_type_food(**kwargs):
    from data_engineering.Transform_data.transform_4_3_window_function_type_food import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_percentage_daily(**kwargs):
    from data_engineering.Transform_data.transform_5_1_percentage_change_daily import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_percentage_user(**kwargs):
    from data_engineering.Transform_data.transform_5_2_percentage_change_user import main
    return main(kwargs['env'])

@task_wrapper
def import_and_run_parquet(**kwargs):
    from data_engineering.Transform_data.transform_6_parquet import main
    return main(kwargs['env'])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration des variables d'environnement pour AWS
env_vars = {
    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'AWS_DEFAULT_REGION': os.getenv('AWS_DEFAULT_REGION'),
    'AWS_BUCKET_NAME': os.getenv('AWS_BUCKET_NAME')
}

dag = DAG(
    'food_analytics_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour Food Analytics',
    schedule_interval='@monthly',
)

# Tâche d'extraction
extract_task = PythonOperator(
    task_id='smart_extract',
    python_callable=import_and_run_smart_extract,
    dag=dag,
)

# Tâches de lecture et combinaison
combine_meal_task = PythonOperator(
    task_id='combine_meal',
    python_callable=import_and_run_combine_meal,
    dag=dag,
)

read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=import_and_run_read_data,
    op_kwargs={'env': env_vars},
    dag=dag,
)

# Tâches de filtrage et groupement
filter_data_task = PythonOperator(
    task_id='filter_data',
    python_callable=import_and_run_filter_data,
    op_kwargs={'env': env_vars},
    dag=dag,
)

group_data_task = PythonOperator(
    task_id='group_data',
    python_callable=import_and_run_group_data,
    op_kwargs={'env': env_vars},
    dag=dag,
)

# Tâches de window functions
window_daily_task = PythonOperator(
    task_id='window_daily',
    python_callable=import_and_run_window_daily,
    op_kwargs={'env': env_vars},
    dag=dag,
)

window_user_task = PythonOperator(
    task_id='window_user',
    python_callable=import_and_run_window_user,
    op_kwargs={'env': env_vars},
    dag=dag,
)

window_type_food_task = PythonOperator(
    task_id='window_type_food',
    python_callable=import_and_run_window_type_food,
    op_kwargs={'env': env_vars},
    dag=dag,
)

# Tâches de percentage change
percentage_daily_task = PythonOperator(
    task_id='percentage_daily',
    python_callable=import_and_run_percentage_daily,
    op_kwargs={'env': env_vars},
    dag=dag,
)

percentage_user_task = PythonOperator(
    task_id='percentage_user',
    python_callable=import_and_run_percentage_user,
    op_kwargs={'env': env_vars},
    dag=dag,
)

# Tâche de conversion en parquet
parquet_task = PythonOperator(
    task_id='parquet_conversion',
    python_callable=import_and_run_parquet,
    op_kwargs={'env': env_vars},
    dag=dag,
)

# Définir l'ordre des tâches
extract_task >> combine_meal_task >> read_data_task >> filter_data_task >> group_data_task
group_data_task >> window_daily_task >> percentage_daily_task
group_data_task >> window_user_task >> percentage_user_task
group_data_task >> window_type_food_task
percentage_daily_task >> parquet_task
percentage_user_task >> parquet_task
