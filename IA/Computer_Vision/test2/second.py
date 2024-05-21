# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 20:33:36 2023

@author: GUEGUEN
"""

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from tensorflow.keras.preprocessing import image
from tensorflow.keras.models import load_model

def preprocess_image(img_path, img_width = 224, img_height=224):
    img = image.load_img(img_path, target_size=(img_width, img_height))
    img_array = image.img_to_array(img)
    img_array = np.expand_dims(img_array, axis=0)
    img_array /= 255.
    return img_array

img_width = 224
img_height=224
def predict_food_category(img_path, model, class_names):
    global img_width
    global img_height
    img = preprocess_image(img_path, img_width, img_height)
    predictions = model.predict(img)
    predicted_class = class_names[np.argmax(predictions)]
    print("Predictions:", predictions)
    return predicted_class

# Charger le modèle
model = load_model('food_recognition_model.h5')

# Charger les noms de classes à partir du fichier Excel
excel_file = 'classe.xlsx'
class_df = pd.read_excel(excel_file, engine='openpyxl')
classes_list = class_df['classes'].tolist()

# Prédire la catégorie d'une nouvelle image
img_path = 'data/test/t15.jpg'
predicted_class = predict_food_category(img_path, model, classes_list)
print(f'Predicted class: {predicted_class}')
