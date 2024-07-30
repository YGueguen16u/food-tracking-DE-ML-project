import sys
import os
import openpyxl

# Ajouter le répertoire racine du projet au sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
print(f"Adding {project_root} to sys.path")
sys.path.append(project_root)

print(f"sys.path: {sys.path}")

# Vérifiez le contenu du répertoire pour s'assurer que les modules sont là
for root, dirs, files in os.walk(os.path.abspath(os.path.dirname(__file__))):
    print(root, dirs, files)

try:
    from data_engineering.sensor_api.fake_data.app_tracker import AppTracker
except ImportError:
    from .app_tracker import AppTracker

from datetime import date
import pandas as pd

def create_users_from_excel(file_path: str) -> list:
    """
    Create user instances from an Excel file
    """
    user_data = pd.read_excel(file_path)
    print(f"User data loaded from {file_path}:\n{user_data.head()}")  # Ajoutez cette ligne pour afficher les premières lignes du fichier Excel
    users = []

    for _, row in user_data.iterrows():
        users.append({
            'nom': row['l_name'],
            'prenom': row['f_name'],
            'age': row['age'],
            'sexe': row['sexe'],
            'user_id': row['user_id'],
            'classe_mangeur': row['classe']
        })

    return users


def create_app() -> AppTracker:
    """
    Create an instance of AppTracker with the given users
    """
    try :
        file_path = "user_table.XLSX"
    except :
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "user_table.XLSX")

    users = create_users_from_excel(file_path)
    print(f"Users created: {users}")  # Ajoutez cette ligne pour vérifier la liste des utilisateurs
    app_tracker = AppTracker(users)
    return app_tracker

if __name__ == "__main__":
    # Code de test
    print("Running test for create_app function...")
    date = date.today().strftime("%Y-%m-%d")
    app = create_app()
    print("app", app.users)
    #print(app.get_connexion(2, date, 4))
    #print(app.get_all_connexion(6, date))