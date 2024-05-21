import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import os
import uuid

# Unique ID initialization just before entering the food items loop
unique_id = uuid.uuid4()

# Selenium options initialization
options = Options()
options.add_argument("--headless")
options.add_argument("window-size=1280,800")
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)

base_url = 'https://www.yazio.com/fr/aliments/'
csv_file_path = r'C:\Users\GUEGUEN\Desktop\WSApp\IM\IA\NLP\raw data\yazio_food.csv'
os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

# Read and extract nutrition column names from the TXT file
with open('html_header_yazio.txt', 'r', encoding='utf-8') as file:
    html_string = file.read()

soup = BeautifulSoup(html_string, 'html.parser')
nutrition_elements = soup.find_all(class_="font-subtitle light mt-0")
nutrient_names = [element.get_text().strip() for element in nutrition_elements]
base_columns = ['id', 'Catégorie', 'Sous-catégorie', 'Nom de l\'aliment']
final_headers = base_columns + nutrient_names

print(final_headers)

# CSV file initialization
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=final_headers)
    writer.writeheader()

    driver.get(base_url)
    wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'img-title-wrapper')))
    category_elements = wait.until(lambda driver: driver.find_elements(By.CLASS_NAME, 'img-title-wrapper'))
    for i, element in enumerate(category_elements):
        # Assuming you want to print the text of each WebElement
        element_text = element.text
        print(f"{i}: {element_text}")

    for category_index in range(len(category_elements)):
        # Refresh category_elements to avoid StaleElementReferenceException
        category_elements = wait.until(lambda driver: driver.find_elements(By.CLASS_NAME, 'img-title-wrapper'))
        category_element = category_elements[category_index]
        category_name = category_element.find_element(By.TAG_NAME, 'span').text
        category_url = category_element.find_element(By.TAG_NAME, 'a').get_attribute('href')
        print(f"cattttttt {category_name}, itération {category_index}" )
        driver.get(category_url)
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'list-item-group')))
        subcategory_elements = driver.find_elements(By.CLASS_NAME, 'list-item-group')

        for i, element in enumerate(subcategory_elements):
            # Assuming you want to print the text of each WebElement
            element_text = element.text
            print(f"{i}: {element_text}")

        for subcategory_index in range(len(subcategory_elements)):
            # Refresh subcategory_elements to avoid StaleElementReferenceException
            subcategory_elements = driver.find_elements(By.CLASS_NAME, 'list-item-group')
            subcategory_element = subcategory_elements[subcategory_index]
            subcategory_name = subcategory_element.find_element(By.TAG_NAME, 'a').text.strip()
            subcategory_url = subcategory_element.find_element(By.TAG_NAME, 'a').get_attribute('href')
            print("\n subbbbbbbb", subcategory_name)
            driver.get(subcategory_url)
            food_items = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.list-item-group .link')))

            for i, element in enumerate(food_items):
                # Assuming you want to print the text of each WebElement
                element_text = element.text
                print(f"{i}: {element_text}")

            for food_item in food_items:
                try:
                    food_name = food_item.text.strip()
                    food_url = food_item.get_attribute('href')
                    driver.get(food_url)
                    print("\n food item : ", food_name)

                    # Générer un nouvel ID unique pour l'aliment courant
                    # unique_id = uuid.uuid4()

                    # Exemple de récupération des informations nutritionnelles (ajustez selon la structure réelle de la page)
                    html_content = driver.page_source  # Obtenez le HTML de la page
                    soup = BeautifulSoup(html_content, 'html.parser')
                    nutrition_elements = soup.find_all(class_="nutrition-row")
                    
                    # Dictionnaire pour stocker les informations nutritionnelles extraites
                    nutrition_info = {}
                    for element in nutrition_elements:
                        # Attempt to find the 'nutrient name' element within the current element
                        nutrient_name_element = element.find(class_="font-subtitle light mt-0")
                        
                        # Ensure the 'nutrient name' element was found before proceeding
                        if nutrient_name_element is not None:
                            nutrient_name = nutrient_name_element.text.strip()
                            
                            # Attempt to find the 'nutrient value' container
                            nutrient_value_container = element.find(class_="serving-calculator-table-value")
                            
                            # Check if the 'nutrient value' container was found before proceeding
                            if nutrient_value_container:
                                nutrient_value = nutrient_value_container.text.strip()
                            else:
                                nutrient_value = 'N/A'  # Or any other default value of your choice
                            
                            # Add the nutrient name and value to your dictionary
                            nutrition_info[nutrient_name] = nutrient_value

                        row_data = {
                            'id': str(unique_id),  # Converti l'UUID en string pour l'écriture dans le CSV
                            'Catégorie': category_name,
                            'Sous-catégorie': subcategory_name,
                            'Nom de l\'aliment': food_name
                        }

                        # Ajouter dynamiquement les informations nutritionnelles à row_data
                        for nutrient_name in nutrient_names:
                            if nutrient_name in nutrition_info:  # Vérifier si l'info est disponible
                                row_data[nutrient_name] = nutrition_info[nutrient_name]
                            else:
                                row_data[nutrient_name] = 'N/A'  # Ou une autre valeur par défaut si non disponible

                        writer.writerow(row_data)
                        unique_id = uuid.uuid4()  # Générer un nouvel ID unique pour le prochain aliment


                except selenium.common.exceptions.StaleElementReferenceException:
                    continue  # Handle the exception, maybe log it or increment a retry counter

driver.quit()
