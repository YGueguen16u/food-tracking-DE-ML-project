# Data Engineering project : Tracking food habits of users

In this project, I created fake users categorized by their food habits 
(meat lover, vegetarian, etc.) and developed an API that generates 
fake consumption data for each user based on their eating profile.

However, the key element for a project like this is having accurate food data 
(e.g., calories, protein, carbohydrates, fats). 
To achieve this, I used web scraping with BeautifulSoup to gather data from the 
Yazio database: https://www.yazio.com/fr/aliments/

The data are generated by an API (sensor_api) that I deployed on Render, 
which produces real-time data.

After extracting the data from the API, I transform it to make it usable 
for further analysis (such as habit analysis, predictions, and 
content-based filtering). 

I also applied some DevOps skills to ensure code quality and results, 
with the goal of integrating 
Continuous Integration/Continuous Deployment (CI/CD)processes.

## 1. Fake User Creation and Food Consumption Data Generation via API and Extraction 

### 1.1 Essaye pratique 

Yo can access the project by following this link : https://github.com/YGueguen16u/sensor_api

You can acces to all foods took by a user during a meal on a day in modifying 
the following link (modify user_id, date, and meal_id)
https://food-tracking-de-ml-project.onrender.com/?user_id=4&year=2024&month=07&day=18&meal_id=1

or getting all foods ate by a user during a whole specific day 
https://food-tracking-de-ml-project.onrender.com/?user_id=4&year=2024&month=07&day=18

Warning :  some fields (date ) must be compulsary

On peut modifier des champs 
Essayer de ne pas tous les remplir ou bien de mettre de la données éronnées (user_id > 21
date dans un mauvais format ... )

Cepepdnat les données sont dans un format peu compréhensible

### 1.2 Le webscrapping avec beautifulsoup et yazio

### 1.3 Comment les données sont faites 

Les données utilisateurs, probabilités suivant leur profil
Les liens entre les différentes tables par la catégorisation des aliments (user, nouritures)

### 1.4 Récupération des données de l'API

que faisons nous ...
Extract
Essaye de stocker les données récupéré dans un bucket S3


## 2. Transform Data



