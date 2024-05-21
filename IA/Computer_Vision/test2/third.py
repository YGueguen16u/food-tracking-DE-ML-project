# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 19:42:21 2023

@author: GUEGUEN
"""

import os
import cv2
import numpy as np
import tensorflow as tf
from object_detection.utils import label_map_util


from object_detection.utils import visualization_utils as vis_util
import sys
sys.path.append('C:/Users/GUEGUEN/models/research/slim')
#sys.path.append("C:/Users/GUEGUEN/anaconda3/Lib/site-packages/object_detection")
sys.path.append("C:\\Users\\GUEGUEN\\Desktop\\WSApp\\IA\\test2\\object_detection")

# Remplacez ces valeurs par les chemins vers votre modèle et votre fichier de mappage des labels
PATH_TO_MODEL = 'C:/Users/GUEGUEN/Desktop/WSApp/IA/test2/my_model/saved_model'
PATH_TO_LABELS = 'C:/Users/GUEGUEN/Desktop/WSApp/IA/test2/label_map/label_map1/label_map.pbtxt'

# Chargez le modèle TensorFlow
model = tf.saved_model.load(PATH_TO_MODEL)

# Chargez le fichier de mappage des labels
category_index = label_map_util.create_category_index_from_labelmap(PATH_TO_LABELS, use_display_name=True)

# Chargez l'image sur laquelle vous voulez effectuer la détection
image_path = 'multi_test/m1.jpg'
image_np = cv2.imread(image_path)

# Redimensionnez l'image à la taille attendue par le modèle (224x224)
image_np = cv2.resize(image_np, (224, 224))

# Convertissez l'image en RGB
image_np = cv2.cvtColor(image_np, cv2.COLOR_BGR2RGB)

# Convertissez l'image en float32 et normalisez les valeurs des pixels entre 0 et 1
image_np = image_np.astype(np.float32) / 255.0


input_tensor = tf.convert_to_tensor(image_np[np.newaxis, ...], dtype=tf.float32) # Changez ici
detections = model(input_tensor)

detections_np = detections.numpy()
print(detections_np.shape)
print("Detections tensor:", detections)
print("Detections tensor shape:", detections.shape)

# Visualisez les résultats de la détection
vis_util.visualize_boxes_and_labels_on_image_array(
    image_np,
    detections_np[0],
    detections_np[1].astype(np.int32),
    detections_np[2],
    category_index,
    use_normalized_coordinates=True,
    line_thickness=2,
    min_score_thresh=0.5)  # Vous pouvez ajuster ce seuil pour filtrer les détections par score


# Affichez l'image avec les détections
image_np = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)
cv2.imshow("Result", image_np)
cv2.waitKey(0)
cv2.destroyAllWindows()

