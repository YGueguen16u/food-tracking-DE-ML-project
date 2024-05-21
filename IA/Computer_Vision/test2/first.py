import numpy as np
import tensorflow as tf
import pandas as pd
from sklearn.metrics import confusion_matrix, classification_report
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input

train_data_dir = 'data/train'
validation_data_dir = 'data/validation'

img_width, img_height = 224, 224
batch_size = 32
epochs = 21

# Lire les classes depuis le fichier Excel
excel_file = 'classe.xlsx'
class_df = pd.read_excel(excel_file, engine='openpyxl')
print(class_df.columns)

classes_list = class_df['classes'].tolist()

# ImageDataGenerator pour l'augmentation des données
train_datagen = ImageDataGenerator(
    preprocessing_function=preprocess_input,
    rotation_range=20,
    width_shift_range=0.2,
    height_shift_range=0.2,
    shear_range=0.2,
    zoom_range=0.2,
    horizontal_flip=True,
    fill_mode='nearest')

validation_datagen = ImageDataGenerator(preprocessing_function=preprocess_input)

train_generator = train_datagen.flow_from_directory(
    train_data_dir,
    target_size=(img_width, img_height),
    batch_size=batch_size,
    class_mode='categorical',
    classes=classes_list)

validation_generator = validation_datagen.flow_from_directory(
    validation_data_dir,
    target_size=(img_width, img_height),
    batch_size=batch_size,
    class_mode='categorical',
    classes=classes_list)

# Charger le modèle MobileNetV2 sans les couches supérieures (sans les couches fully connected)
base_model = MobileNetV2(weights='imagenet', include_top=False, input_shape=(img_width, img_height, 3))

# Ajouter de nouvelles couches fully connected
x = base_model.output
x = GlobalAveragePooling2D()(x)
x = Dense(1024, activation='relu')(x)
predictions = Dense(train_generator.num_classes, activation='softmax')(x)

model = Model(inputs=base_model.input, outputs=predictions)

# Geler les couches du modèle de base pour ne pas les entraîner
for layer in base_model.layers:
    layer.trainable = False

model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),
              loss='categorical_crossentropy',
              metrics=['accuracy'])

model.fit(
    train_generator,
    steps_per_epoch=train_generator.samples // batch_size,
    epochs=epochs,
    validation_data=validation_generator,
    validation_steps=validation_generator.samples // batch_size)

model.save('food_recognition_model.h5')

saved_model_path = "C:/Users/GUEGUEN/Desktop/WSApp/IA/test2/my_model/saved_model"
tf.saved_model.save(model, saved_model_path)

# Prédire les classes pour l'ensemble de validation
y_pred = model.predict(validation_generator)
y_pred_classes = np.argmax(y_pred, axis=1)

# Obtenir les véritables étiquettes (labels) de l'ensemble de validation
y_true = validation_generator.classes

# Calculer la matrice de confusion
confusion_mat = confusion_matrix(y_true, y_pred_classes)

print("Matrice de confusion :")
print(confusion_mat)

# Obtenir les labels uniques de y_true
unique_labels = np.unique(y_true)

# Inverser le dictionnaire
