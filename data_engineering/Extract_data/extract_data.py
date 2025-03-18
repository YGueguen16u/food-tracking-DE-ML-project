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
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def upload_to_s3(filepath):
    """
    Upload a file to S3 bucket in the raw_data_ingestion/[YYYY]/[MM]/ directory structure
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
    print("Erreur: La date passée en paramètre n'est pas valide. Utilisez le format AAAA-MM-JJ.")
    sys.exit(1)

print(f"Date passée en paramètre: {date_param}")

def request_our_api(user_id, year, month, day):
    url = f"https://food-tracking-de-ml-project.onrender.com/?user_id={user_id}&year={year}&month={month}&day={day}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur lors de la requête de notre API: {response.status_code}")
        return None


def generate_csv(all_data, year, month, user_id):
    if not os.path.exists('data/raw'):
        print("Création du répertoire 'data/raw'")
        os.makedirs('data/raw')

    filename = f"data/raw/{year}_{month:02d}_user_{user_id}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "meal_id", "date", "heure", "aliment_id", "quantity"])
        for data in all_data:
            if "user_id" in data and "meal_id" in data and "heure_repas" in data and "aliment_id" in data and "quantity" in data:
                print(f"Structure correcte des données pour user_id {user_id} à la date {year}-{month}")
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
                print(f"Structure incorrecte pour user_id {user_id} à la date {year}-{month}")
    
    # Upload the generated CSV to S3
    upload_to_s3(filename)

user_ids = [i for i in range(1, 20)]  # Liste des user_id à itérer
start_date = datetime.strptime(date_param, "%Y-%m-%d")
end_date = datetime.today()
current_date = start_date

# Calcul du nombre total d'itérations pour la barre de progression
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