import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import random

# Classe de base Mangeur
class User:
    """
    Class User
    """
    def __init__(self, 
                 nom: str, 
                 prenom: str, 
                 age: int, 
                 sexe: str, 
                 user_id: int, 
                 classe_mangeur: str,
                 type_food: str, 
                ) -> None:
        """
        Initialize a user with their attributes
        """
        self.nom = nom
        self.prenom = prenom
        self.age = age
        self.sexe = sexe
        self.user_id = user_id
        self.classe_mangeur = classe_mangeur
        self.facteur_calories = 1.2 if sexe == 'homme' else 1.0
        self.heures_repas = {}
        self.intervalles_calories = {}
        self.type_food_file = type_food  # Remplacez par le chemin réel
        self.probabilites_df = pd.read_excel(self.type_food_file)
        self.aliments_consomme = []
        
    def generer_heures_connexion(self):
        """
        Génère les heures de connexion pour chaque repas avec une variation aléatoire.

        Pour chaque repas défini dans `self.heures_repas`, cette méthode génère une heure de connexion
        avec une variation aléatoire autour de l'heure prévue. Si la classe de mangeur est 'random',
        la variation peut aller de -60 à +300 minutes. Sinon, la variation est limitée à -60 à +60 minutes.

        Returns:
            list of tuple: Une liste de tuples où chaque tuple contient le numéro du repas et l'heure de connexion variée.
                        Exemple: [(1, datetime), (2, datetime), ...]
        """
        heures_de_connexion = []
        for repas, heure in self.heures_repas.items():
            heure_reelle = datetime.strptime(heure, '%H:%M')
            if self.classe_mangeur == 'random':
                variation = timedelta(minutes=random.randint(-60, 300))
            else :
                variation = timedelta(minutes=random.randint(-60, 60))
            heure_variee = heure_reelle + variation
            heures_de_connexion.append((repas, heure_variee))
        return heures_de_connexion

    def choisir_types_aliments(self, repas):
        """
        Choisit les types d'aliments à consommer pour un repas donné en fonction des probabilités.

        Utilise les colonnes spécifiques aux moyennes et écarts types pour le repas pour générer 
        des probabilités et choisir aléatoirement les types d'aliments en fonction de ces probabilités.

        Args:
            repas (int): Le numéro du repas (1 pour petit déjeuner, 2 pour déjeuner, etc.)

        Returns:
            np.ndarray: Un tableau des types d'aliments choisis.
        """
        repas_col_avg = f'Meal_{repas}_avg'
        repas_col_std = f'Meal_{repas}_std'
        
        types = self.probabilites_df['Types']
        moyennes = self.probabilites_df[repas_col_avg]
        std_devs = self.probabilites_df[repas_col_std]

        probabilites = np.random.normal(moyennes, std_devs)
        probabilites = np.clip(probabilites, 0, None)  # S'assurer que les probabilités ne sont pas négatives

        total_probabilite = np.sum(probabilites)
        if total_probabilite > 0:
            probabilites = probabilites / total_probabilite  # Normaliser pour que la somme des probabilités soit 1

        types_choisis = np.random.choice(types, size=len(types), p=probabilites)
        return types_choisis

    def determiner_quantite(self, type_aliment):
        """
        Détermine la quantité d'un aliment en fonction du type d'aliment et de la classe de mangeur.

        Returns:
            int: La quantité d'aliment.
        """
        # 5% de chances que la quantité soit nulle
        if random.random() < 0.04:
            return 0
        elif random.random() > 0.99:
            return random.randint(100, 100000)

        quantite = 1  # Quantité par défaut

        if self.classe_mangeur == 'meat_lover':
            if type_aliment in ['Viande', 'Poisson', 'Oeuf'] and random.random() < 0.30:
                quantite = random.randint(2, 5)
        elif self.classe_mangeur in ['vegan', 'vegetarian']:
            if type_aliment in ['Légumes', 'Fruit', 'Légumineuse'] and random.random() < 0.30:
                quantite = random.randint(2, 5)
        elif self.classe_mangeur == 'standard':
            if type_aliment in ['viande', 'poisson', 'oeuf', 'Légumes', 'Fruit',
                                'Légumineuse'] and random.random() < 0.30:
                quantite = random.randint(2, 5)
        elif self.classe_mangeur == 'random':
            if random.random() < 0.30:
                quantite = random.randint(2, 5)

        return quantite

    def selectionner_aliments(self, aliments_df, types_choisis, repas, min_calories, max_calories):
        """
        Sélectionne les aliments à consommer en fonction des types choisis et des contraintes caloriques.

        Les aliments sont choisis aléatoirement à partir des types sélectionnés, jusqu'à ce que le 
        minimum de calories soit atteint. Si les calories totales dépassent le maximum autorisé,
        des aliments sont retirés en fonction de leur probabilité.

        Args:
            aliments_df (pd.DataFrame): Le DataFrame contenant les informations sur les aliments.
            types_choisis (np.ndarray): Les types d'aliments choisis pour le repas.
            repas (int): Le numéro du repas (1 pour petit déjeuner, 2 pour déjeuner, etc.)
            min_calories (float): Le nombre minimum de calories à consommer pour ce repas.
            max_calories (float): Le nombre maximum de calories à consommer pour ce repas.

        Returns:
            list of dict: Une liste de dictionnaires représentant les aliments sélectionnés.
        """
        min_calories *= self.facteur_calories
        max_calories *= self.facteur_calories
        total_calories = 0
        aliments_selectionnes = []

        exceed_max_calories = random.random() < 0.3  # 30% de chances de pouvoir dépasser max_calories

        for type_aliment in types_choisis:
            aliments_du_type = aliments_df[aliments_df['Type'] == type_aliment]
            aliment_choisi = aliments_du_type.sample(n=1).iloc[0].to_dict()
            aliment_choisi['Repas'] = repas  # Ajouter l'information du repas

            # Déterminer la quantité de l'aliment
            quantite = self.determiner_quantite(type_aliment)
            aliment_choisi['Quantite'] = quantite
            if quantite >= 10:
                exceed_max_calories = 1
            total_calories += aliment_choisi['Valeur calorique'] * quantite
            aliments_selectionnes.append(aliment_choisi)

            if total_calories >= min_calories and (exceed_max_calories or total_calories <= max_calories):
                break

        # Si le total dépasse max_calories et exceed_max_calories est False, retirer des aliments avec les plus faibles probabilités
        if not exceed_max_calories and total_calories > max_calories:
            while total_calories > max_calories:
                aliments_selectionnes.sort(key=lambda x: self.probabilite_aliment(x['Type'], x['Repas']))
                aliment_a_retirer = aliments_selectionnes.pop(0)
                total_calories -= aliment_a_retirer['Valeur calorique'] * aliment_a_retirer['Quantite']

        return aliments_selectionnes

    def probabilite_aliment(self, type_aliment, repas):
        """
        Récupère la probabilité moyenne d'un type d'aliment pour un repas spécifique.

        Args:
            type_aliment (str): Le type d'aliment dont on veut connaître la probabilité.
            repas (int): Le numéro du repas (1 pour petit déjeuner, 2 pour déjeuner, etc.)

        Returns:
            float: La probabilité moyenne de l'aliment pour le repas donné.
        """
        repas_col_avg = f'Meal_{repas}_avg'
        moyenne = self.probabilites_df[self.probabilites_df['Types'] == type_aliment][repas_col_avg].values[0]
        return moyenne
        
    def simulate_daily_activity(self, business_date: date, aliments_df: pd.DataFrame):
        """
        Simule les activités alimentaires quotidiennes de l'utilisateur pour une date donnée.

        Génère les heures de connexion, choisit les types d'aliments pour chaque repas et sélectionne 
        les aliments consommés en fonction des probabilités et des contraintes caloriques.

        Args:
            business_date (date): La date pour laquelle simuler les activités.
            aliments_df (pd.DataFrame): Le DataFrame contenant les informations sur les aliments.
        """
        heures_de_connexion = self.generer_heures_connexion()
        self.aliments_consomme = []

        for repas, heure in heures_de_connexion:
            types_choisis = self.choisir_types_aliments(repas)
            aliments_selectionnes = self.selectionner_aliments(
                aliments_df, types_choisis, repas,
                self.intervalles_calories[repas][0],
                self.intervalles_calories[repas][1]
            )
            self.aliments_consomme.extend(aliments_selectionnes)
            print(f"{self.nom} s'est connecté à {heure.strftime('%Y-%m-%d %H:%M:%S')}")
            for aliment in aliments_selectionnes:
                quantite = aliment.get('Quantite', 1)
                calories_par_portion = aliment['Valeur calorique']
                calories_totales = calories_par_portion * quantite
                print(f"{self.nom} a consommé {quantite} portion(s) de {aliment['Aliment']} ({calories_totales} calories au total) pour le repas {repas}")


    def get_daily_activity(self, business_date: date) -> dict:
        """
        Récupère le journal d'activité alimentaire de l'utilisateur pour une date spécifique.

        Args:
            business_date (date): La date pour laquelle récupérer le journal d'activité.

        Returns:
            dict: Un dictionnaire contenant la date et la liste des aliments consommés.
        """
        return {
            'date': business_date,
            'aliments_consomme': self.aliments_consomme
        }

# Sous-classe pour les mangeurs standard
class Standard(User):
    """
    Classe représentant un mangeur standard.

    Hérite de la classe User et initialise les attributs spécifiques pour un mangeur standard,
    y compris les heures de repas et les intervalles de calories pour chaque repas.

    Args:
        nom (str): Le nom de l'utilisateur.
        prenom (str): Le prénom de l'utilisateur.
        age (int): L'âge de l'utilisateur.
        sexe (str): Le sexe de l'utilisateur ('homme' ou 'femme').
        user_id (int): L'identifiant unique de l'utilisateur.
    """
    def __init__(self, nom, prenom, age, sexe, user_id):
        type_food_file = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\user\standard_class.xlsx"
        super().__init__(nom, prenom, age, sexe, user_id, 'standard', type_food_file)
        self.heures_repas = {
            1: '08:00',  # petit_dejeuner
            2: '12:00',  # dejeuner
            3: '16:00',  # gouter
            4: '20:00'   # diner
        }
        self.intervalles_calories = {
            1: (300, 500),  # petit_dejeuner
            2: (600, 800),  # dejeuner
            3: (200, 300),  # gouter
            4: (500, 700)   # diner
        }

# Classe MangeurStandard héritant de Mangeur
class MeatLover(User):
    """
    Classe représentant un mangeur amateur de viande.

    Hérite de la classe User et initialise les attributs spécifiques pour un mangeur amateur de viande,
    y compris les heures de repas et les intervalles de calories pour chaque repas.

    Args:
        nom (str): Le nom de l'utilisateur.
        prenom (str): Le prénom de l'utilisateur.
        age (int): L'âge de l'utilisateur.
        sexe (str): Le sexe de l'utilisateur ('homme' ou 'femme').
        user_id (int): L'identifiant unique de l'utilisateur.
    """
    def __init__(self, nom, prenom, age, sexe, user_id):
        type_food_file = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\user\meat_lover_class.xlsx"
        super().__init__(nom, prenom, age, sexe, user_id, 'meat_lover', type_food_file)        
        self.heures_repas = {
            1: '08:00',  # petit_dejeuner
            2: '12:00',  # dejeuner
            3: '16:00',  # gouter
            4: '20:00'   # diner
        }
        self.intervalles_calories = {
            1: (400, 600),  # petit_dejeuner
            2: (700, 900),  # dejeuner
            3: (300, 500),  # gouter
            4: (600, 800)   # diner
        }

class Vegetarian(User):
    """
    Classe représentant un mangeur végétarien.

    Hérite de la classe User et initialise les attributs spécifiques pour un mangeur végétarien,
    y compris les heures de repas et les intervalles de calories pour chaque repas.

    Args:
        nom (str): Le nom de l'utilisateur.
        prenom (str): Le prénom de l'utilisateur.
        age (int): L'âge de l'utilisateur.
        sexe (str): Le sexe de l'utilisateur ('homme' ou 'femme').
        user_id (int): L'identifiant unique de l'utilisateur.
    """
    def __init__(self, nom, prenom, age, sexe, user_id):
        type_food_file = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\user\vegetarian_class.xlsx"
        super().__init__(nom, prenom, age, sexe, user_id, 'vegetarian', type_food_file)
        self.heures_repas = {
            1: '08:00',  # petit_dejeuner
            2: '12:00',  # dejeuner
            3: '16:00',  # gouter
            4: '20:00'   # diner
        }
        self.intervalles_calories = {
            1: (300, 400),  # petit_dejeuner
            2: (500, 700),  # dejeuner
            3: (100, 200),  # gouter
            4: (450, 600)   # diner
        }

class Vegan(User):
    """
    Classe représentant un mangeur végétalien.

    Hérite de la classe User et initialise les attributs spécifiques pour un mangeur végétalien,
    y compris les heures de repas et les intervalles de calories pour chaque repas.

    Args:
        nom (str): Le nom de l'utilisateur.
        prenom (str): Le prénom de l'utilisateur.
        age (int): L'âge de l'utilisateur.
        sexe (str): Le sexe de l'utilisateur ('homme' ou 'femme').
        user_id (int): L'identifiant unique de l'utilisateur.
    """
    def __init__(self, nom, prenom, age, sexe, user_id):
        type_food_file = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\user\vegan_class.xlsx"
        super().__init__(nom, prenom, age, sexe, user_id, 'vegan', type_food_file)
        self.heures_repas = {
            1: '08:00',  # petit_dejeuner
            2: '12:00',  # dejeuner
            3: '16:00',  # gouter
            4: '20:00'   # diner
        }
        self.intervalles_calories = {
            1: (250, 350),  # petit_dejeuner
            2: (500, 700),  # dejeuner
            3: (100, 200),  # gouter
            4: (400, 500)   # diner
        }

class Fasting(User):
    """
    Classe représentant un mangeur pratiquant le jeûne intermittent.

    Hérite de la classe User et initialise les attributs spécifiques pour un mangeur pratiquant le jeûne,
    y compris les heures de repas et les intervalles de calories pour chaque repas.

    Args:
        nom (str): Le nom de l'utilisateur.
        prenom (str): Le prénom de l'utilisateur.
        age (int): L'âge de l'utilisateur.
        sexe (str): Le sexe de l'utilisateur ('homme' ou 'femme').
        user_id (int): L'identifiant unique de l'utilisateur.
    """
    def __init__(self, nom, prenom, age, sexe, user_id):
        type_food_file = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\user\fasting_class.xlsx"
        super().__init__(nom, prenom, age, sexe, user_id, 'fasting', type_food_file)
        self.heures_repas = {
            1: '12:00',  # dejeuner
            2: '18:00',  # diner
        }
        self.intervalles_calories = {
            1: (1000, 1200),  # dejeuner
            2: (800, 1000)   # diner
        }

class Random(User):
    """
    Classe représentant un mangeur aléatoire.

    Hérite de la classe User et initialise les attributs spécifiques pour un mangeur aléatoire,
    y compris les heures de repas et les intervalles de calories pour chaque repas.

    Args:
        nom (str): Le nom de l'utilisateur.
        prenom (str): Le prénom de l'utilisateur.
        age (int): L'âge de l'utilisateur.
        sexe (str): Le sexe de l'utilisateur ('homme' ou 'femme').
        user_id (int): L'identifiant unique de l'utilisateur.
    """
    def __init__(self, nom, prenom, age, sexe, user_id):
        type_food_file = r"C:\Users\GUEGUEN\Desktop\WSApp\IM\DB\user\random_eater_class.xlsx"
        super().__init__(nom, prenom, age, sexe, user_id, 'random', type_food_file)
        self.heures_repas = {
            1: '13:00',  
        }
        self.intervalles_calories = {
            1: (900, 4500),  # petit_dejeuner
        }

# Création d'une instance utilisateur en fonction de la classe de mangeur
def create_user_instance(user_data: dict) -> User:
    """
    Crée une instance d'utilisateur en fonction des données fournies et de la classe de mangeur.

    Cette fonction crée une instance de la classe appropriée d'utilisateur (`Standard`, `MeatLover`, 
    `Vegetarian`, `Vegan`, `Fasting`, ou `Random`) en fonction de la classe de mangeur spécifiée dans 
    `user_data`. Si la classe de mangeur spécifiée n'est pas trouvée, elle retourne une instance de base `User`.

    Args:
        user_data (dict): Un dictionnaire contenant les informations de l'utilisateur, y compris 
                          la clé 'classe_mangeur' qui spécifie la classe de mangeur de l'utilisateur.

    Returns:
        User: Une instance de la classe appropriée de mangeur ou une instance de la classe de base `User`.
    """
    classes_mangeurs = {
        'standard': Standard,
        'meat_lover': MeatLover,
        'vegetarian': Vegetarian,
        'vegan': Vegan,
        'fasting': Fasting,
        'random': Random
    }
    classe_mangeur = user_data.pop('classe_mangeur')
    user_class = classes_mangeurs.get(classe_mangeur, User)
    return user_class(**user_data)