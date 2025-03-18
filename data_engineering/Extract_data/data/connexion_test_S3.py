import boto3
import os

# Créer un client S3
s3 = boto3.client('s3')

# Nom du bucket
bucket_name = 'food-tracking-data'

# Dossier local à uploader (chemin relatif ou absolu)
local_directory = 'raw'

# Parcourir tous les fichiers du dossier local
for root, dirs, files in os.walk(local_directory):
    for file in files:
        # Chemin complet du fichier local
        local_file_path = os.path.join(root, file)

        # Clé S3 : Maintenir la structure du dossier en S3
        relative_path = os.path.relpath(local_file_path, local_directory)
        s3_key = os.path.join('raw', relative_path)

        # Upload du fichier
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Fichier {local_file_path} envoyé à {bucket_name}/{s3_key}")

print("Tous les fichiers du dossier 'raw' ont été envoyés.")
