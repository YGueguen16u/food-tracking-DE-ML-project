import requests
from bs4 import BeautifulSoup
import re
import os
import pandas as pd

def download_images(url, folder, num_images, page=1):
    if not os.path.exists(folder):
        os.makedirs(folder)

    response = requests.get(f"{url}&page={page}")
    soup = BeautifulSoup(response.content, "html.parser")
    images = soup.find_all("img", {"src": re.compile(r"(https://.*\.jpg)")})

    print(f"Images récupérées (page {page}): {len(images)}")

    for i, image in enumerate(images):
        if i >= num_images:
            break
        img_url = image["src"]
        img_name = os.path.join(folder, f"image_{page}_{i}.jpg")
        with open(img_name, "wb") as f:
            img_data = requests.get(img_url).content
            f.write(img_data)
        print(f"Image {page}_{i} téléchargée: {img_url}")

def read_aliments_from_excel(file_path):
    df = pd.read_excel(file_path, header=None)
    aliments = df.iloc[:, 0].tolist()
    return aliments

file_path = "aliment.xlsx"
aliments = read_aliments_from_excel(file_path)
base_url = "https://www.istockphoto.com/fr/photos/{0}?phrase={0}&sort=mostpopular"
folder = "Images_nourriture"
num_images = 100

for aliment in aliments:
    print(f"Téléchargement des images pour {aliment}")
    aliment_folder = os.path.join(folder, aliment)
    aliment_url = base_url.format(aliment)

    images_downloaded = 0
    page = 1
    while images_downloaded < num_images:
        images_to_download = min(num_images - images_downloaded, 100) # iStock affiche 100 images par page
        download_images(aliment_url, aliment_folder, images_to_download, page)
        images_downloaded += images_to_download
        page += 1
