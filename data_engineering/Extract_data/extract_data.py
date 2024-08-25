import sys
from datetime import datetime, timedelta
import requests
import csv
import os
import random
import time
# import boto3
# from botocore.exceptions import NoCredentialsError

# Set the seed for reproducibility
random.seed(42)

def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
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
        os.makedirs('data/raw')

    filename = f"data/raw/{year}_{month:02d}_user_{user_id}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "meal_id", "date", "heure", "aliment_id", "quantity"])
        for data in all_data:
            for i in range(len(data["user_id"])):
                # Ajouter de la donnée non fiable
                aliment_id = data["aliment_id"][i] if random.random() > 0.01 else None
                writer.writerow([
                    data["user_id"][i],
                    data["meal_id"][i],
                    data["heure_repas"][i].split(" ")[0],
                    data["heure_repas"][i].split(" ")[1],
                    aliment_id,
                    data["quantity"][i]
                ])


user_ids = [i for i in range(1, 20)]  # Liste des user_id à itérer
start_date = datetime.strptime(date_param, "%Y-%m-%d")
end_date = datetime.today()
current_date = start_date

for user_id in user_ids:
    current_date = start_date
    monthly_data = {}
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day

        start_time = time.time()  # Démarrer le timer
        data = request_our_api(user_id, year, month, day)
        end_time = time.time()  # Arrêter le timer
        elapsed_time = end_time - start_time
        print(f"User ID {user_id} on {current_date.strftime('%Y-%m-%d')} took {elapsed_time:.2f} seconds")

        if data:
            month_key = f"{year}_{month:02d}"
            if month_key not in monthly_data:
                monthly_data[month_key] = []
            monthly_data[month_key].append(data)

        current_date += timedelta(days=1)

    for month_key, data_list in monthly_data.items():
        year, month = map(int, month_key.split('_'))
        generate_csv(data_list, year, month, user_id)


"""
{
"user_id":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],
"meal_id":[1,1,1,1,1,2,2,2,2,2,2,3,3,3,3,4,4,4],
"heure_repas":["2024-07-20 07:52:00","2024-07-20 07:52:00","2024-07-20 07:52:00","2024-07-20 07:52:00","2024-07-20 07:52:00","2024-07-20 12:00:00","2024-07-20 12:00:00","2024-07-20 12:00:00","2024-07-20 12:00:00","2024-07-20 12:00:00","2024-07-20 12:00:00","2024-07-20 15:19:00","2024-07-20 15:19:00","2024-07-20 15:19:00","2024-07-20 15:19:00","2024-07-20 20:56:00","2024-07-20 20:56:00","2024-07-20 20:56:00"],
"aliment_id":[103,251,244,100,345,329,100,55,455,505,141,341,241,255,236,430,56,441],
"quantity":[1,5,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,3]
}
"""