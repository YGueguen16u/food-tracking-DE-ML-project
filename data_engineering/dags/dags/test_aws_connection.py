from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

def test_aws_connection():
    try:
        # Test AWS credentials
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_DEFAULT_REGION')
        bucket_name = os.getenv('AWS_BUCKET_NAME')

        if not all([aws_access_key, aws_secret_key, aws_region, bucket_name]):
            raise ValueError("Missing AWS credentials in environment variables")

        # Try to create an S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        # Try to list buckets
        response = s3.list_buckets()
        logger.info("Successfully connected to AWS!")
        logger.info(f"Available buckets: {[bucket['Name'] for bucket in response['Buckets']]}")
        
        # Try to list objects in the specified bucket
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        logger.info(f"Successfully accessed bucket: {bucket_name}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing AWS connection: {str(e)}")
        raise

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
    'test_aws_connection',
    default_args=default_args,
    description='Test AWS Connection',
    schedule_interval=None,
)

test_task = PythonOperator(
    task_id='test_aws_connection',
    python_callable=test_aws_connection,
    dag=dag,
)
