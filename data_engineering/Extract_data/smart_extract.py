import sys
import os
from datetime import datetime
import subprocess
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

def get_latest_date_from_s3():
    """
    Parcourt les fichiers dans S3 pour trouver la date la plus récente
    Retourne un tuple (année, mois) ou None si aucun fichier trouvé
    """
    try:
        s3 = S3Manager()
        files = s3.list_files("raw_data_ingestion/")
        
        if not files:
            print("Aucun fichier trouvé dans S3. Démarrage depuis le début...")
            return None
            
        # Extraction des dates (YYYY_MM) des noms de fichiers
        dates = []
        for file in files:
            try:
                # Le nom du fichier est le dernier élément après '/'
                filename = file.split('/')[-1]
                year, month = map(int, filename.split('_')[:2])
                dates.append((year, month))
            except (ValueError, IndexError):
                continue
        
        if not dates:
            return None
            
        # Trouve la date la plus récente
        latest_date = max(dates)
        print(f"Date la plus récente trouvée : {latest_date[0]}-{latest_date[1]:02d}")
        return latest_date
        
    except Exception as e:
        print(f"Erreur lors de la recherche de la dernière date : {str(e)}")
        return None

def get_start_date():
    """
    Détermine la date de départ pour l'extraction
    """
    latest_date = get_latest_date_from_s3()
    
    if latest_date is None:
        # Si aucune date trouvée, commence un an avant aujourd'hui
        start_date = datetime.now() - relativedelta(years=1)
        print(f"Aucune date trouvée. Démarrage depuis : {start_date.strftime('%Y-%m-%d')}")
    else:
        # Prend le premier jour du mois suivant
        year, month = latest_date
        start_date = datetime(year, month, 1) + relativedelta(months=1)
        print(f"Reprise de l'extraction à partir du : {start_date.strftime('%Y-%m-%d')}")
    
    return start_date.strftime("%Y-%m-%d")

def run_extract_data(start_date):
    """
    Lance le script extract_data.py avec la date de départ
    """
    script_path = os.path.join(os.path.dirname(__file__), 'extract_data.py')
    print(f"Lancement de l'extraction depuis {start_date}...")
    
    try:
        subprocess.run([sys.executable, script_path, start_date], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de extract_data.py : {str(e)}")
        sys.exit(1)

def main():
    """
    Fonction principale pour l'extraction des données
    """
    start_date = get_start_date()
    run_extract_data(start_date)

if __name__ == "__main__":
    main()
