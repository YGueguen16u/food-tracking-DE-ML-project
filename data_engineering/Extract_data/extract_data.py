# data_engineering/extract_data.py

"""
Data Extraction Module for Food Tracking Project

This module handles the extraction of food tracking data from the API and uploads it to AWS S3.
It processes data for multiple users over a specified date range and organizes it into CSV files
with a structured directory hierarchy in S3.

Features:
    - Extracts data from the food tracking API
    - Processes multiple users in parallel
    - Organizes data by year/month
    - Uploads data to S3 with proper directory structure
    - Includes progress tracking and logging

Typical usage:
    python extract_data.py 2024-01-01

The script requires a valid start date in YYYY-MM-DD format as an argument.
"""
import sys
from datetime import datetime, timedelta
import requests
import csv
import os
import random
import time
import sys
import logging
from tqdm import tqdm
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_extraction.log'),
        logging.StreamHandler()
    ]
)

# Set the seed for reproducibility
random.seed(42)

def is_valid_date(date_str):
    """
    Validate if a string represents a valid date in YYYY-MM-DD format.
    
    Args:
        date_str (str): Date string to validate
        
    Returns:
        bool: True if the date is valid, False otherwise
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def upload_to_s3(filepath):
    """
    Upload a file to S3 bucket in the raw_data_ingestion/[YYYY]/[MM]/ directory structure.
    
    Args:
        filepath (str): Local path to the file to upload
        
    Returns:
        bool: True if upload was successful, False otherwise
        
    The function automatically extracts year and month from the filename
    (expected format: YYYY_MM_user_X.csv) and creates the appropriate
    directory structure in S3: raw_data_ingestion/YYYY/MM/filename
    
    Example:
        Local file: data/raw/2024_01_user_1.csv
        S3 path: raw_data_ingestion/2024/01/2024_01_user_1.csv
    """
    try:
        s3 = S3Manager()
        filename = os.path.basename(filepath)
        
        # Extract year and month from filename (format: YYYY_MM_user_X.csv)
        year, month = filename.split('_')[:2]
        
        # Create a structured path: raw_data_ingestion/YYYY/MM/filename
        s3_key = f"raw_data_ingestion/{year}/{month}/{filename}"
        
        logging.info(f"Uploading {filename} to S3 in {s3_key}...")
        success = s3.upload_file(filepath, s3_key)
        
        if success:
            logging.info(f"Successfully uploaded {filename} to S3 path: {s3_key}")
            return True
        else:
            logging.error(f"Failed to upload {filename} to S3")
            return False
    except Exception as e:
        logging.error(f"Error uploading to S3: {str(e)}")
        return False

if len(sys.argv) != 2:
    print("Usage: python script.py <date>")
    sys.exit(1)

date_param = sys.argv[1]

if not is_valid_date(date_param):
    print("Error: The date parameter is not valid. Use the format YYYY-MM-DD.")
    sys.exit(1)

print(f"Date parameter: {date_param}")

def request_our_api(user_id, year, month, day):
    """
    Request data from the food tracking API for a specific user and date.
    
    Args:
        user_id (int): ID of the user to fetch data for
        year (int): Year of the data
        month (int): Month of the data
        day (int): Day of the data
        
    Returns:
        dict: JSON response from the API if successful, None otherwise
    """
    url = f"https://food-tracking-de-ml-project.onrender.com/?user_id={user_id}&year={year}&month={month}&day={day}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error requesting our API: {response.status_code}")
        return None


def generate_csv(all_data, year, month, user_id):
    """
    Generate a CSV file from the collected data and upload it to S3.
    
    Args:
        all_data (list): List of data dictionaries containing user food tracking information
        year (int): Year of the data
        month (int): Month of the data
        user_id (int): ID of the user
        
    The function creates a CSV file with the following structure:
    - Filename format: YYYY_MM_user_X.csv
    - Columns: user_id, meal_id, date, heure, aliment_id, quantity
    - Directory: data/raw/
    
    After creation, the file is automatically uploaded to S3 in the appropriate
    directory structure: raw_data_ingestion/YYYY/MM/
    """
    if not os.path.exists('data/raw'):
        print("Creating 'data/raw' directory")
        os.makedirs('data/raw')

    filename = f"data/raw/{year}_{month:02d}_user_{user_id}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "meal_id", "date", "heure", "aliment_id", "quantity"])
        for data in all_data:
            if "user_id" in data and "meal_id" in data and "heure_repas" in data and "aliment_id" in data and "quantity" in data:
                print(f"Correct data structure for user_id {user_id} at date {year}-{month}")
                for i in range(len(data["user_id"])):
                    aliment_id = data["aliment_id"][i] if random.random() > 0.01 else None
                    writer.writerow([
                        data["user_id"][i],
                        data["meal_id"][i],
                        data["heure_repas"][i].split(" ")[0],
                        data["heure_repas"][i].split(" ")[1],
                        aliment_id,
                        data["quantity"][i]
                    ])
            else:
                print(f"Incorrect data structure for user_id {user_id} at date {year}-{month}")
    
    # Upload the generated CSV to S3
    upload_to_s3(filename)

user_ids = [i for i in range(1, 20)]  # List of user_ids to iterate over
start_date = datetime.strptime(date_param, "%Y-%m-%d")
end_date = datetime.today()
current_date = start_date

# Calculate total number of iterations for progress bar
total_days = (end_date - start_date).days + 1
total_iterations = len(user_ids) * total_days

with tqdm(total=total_iterations, desc="Processing data") as pbar:
    for user_id in user_ids:
        current_date = start_date
        monthly_data = {}
        logging.info(f"Starting processing for user {user_id}")
        
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day

            start_time = time.time()
            data = request_our_api(user_id, year, month, day)
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logging.info(f"Processing: User {user_id} - Date {current_date.strftime('%Y-%m-%d')} - Time: {elapsed_time:.2f}s")

            if data:
                month_key = f"{year}_{month:02d}"
                if month_key not in monthly_data:
                    monthly_data[month_key] = []
                monthly_data[month_key].append(data)

            current_date += timedelta(days=1)
            pbar.update(1)

        for month_key, data_list in monthly_data.items():
            year, month = map(int, month_key.split('_'))
            logging.info(f"Generating CSV for user {user_id} - {year}/{month:02d}")
            generate_csv(data_list, year, month, user_id)

logging.info("Data extraction and upload completed!")