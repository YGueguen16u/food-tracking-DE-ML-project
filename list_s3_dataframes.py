import pandas as pd
import io
import os
from aws_s3.connect_s3 import S3Manager
import json

def should_process_folder(folder_name):
    """Vérifie si le dossier doit être traité"""
    excluded_folders = {'raw_data_ingestion', 'test', 'parquet'}
    return not any(excluded in folder_name.lower() for excluded in excluded_folders)

def get_excel_columns_from_s3(s3_manager, s3_key):
    """Lit un fichier Excel depuis S3 et retourne ses colonnes"""
    try:
        # Télécharger le fichier depuis S3
        response = s3_manager.s3_client.get_object(Bucket=s3_manager.bucket_name, Key=s3_key)
        
        # Lire le contenu avec pandas
        if s3_key.endswith('.xlsx') or s3_key.endswith('.xls'):
            df = pd.read_excel(io.BytesIO(response['Body'].read()))
        elif s3_key.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
        else:
            return None
            
        return list(df.columns)
    except Exception as e:
        print(f"Erreur lors de la lecture de {s3_key}: {str(e)}")
        return None

def main():
    # Initialiser la connexion S3
    s3_manager = S3Manager()
    
    # Dictionnaire pour stocker les informations des DataFrames
    dataframes_info = {}
    
    try:
        # Lister tous les objets dans le bucket
        paginator = s3_manager.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_manager.bucket_name)
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                s3_key = obj['Key']
                
                # Vérifier si c'est un fichier Excel ou CSV et s'il n'est pas dans un dossier exclu
                if (s3_key.endswith(('.xlsx', '.xls', '.csv')) and 
                    should_process_folder(s3_key)):
                    
                    print(f"Traitement de {s3_key}...")
                    columns = get_excel_columns_from_s3(s3_manager, s3_key)
                    
                    if columns:
                        folder_path = os.path.dirname(s3_key)
                        file_name = os.path.basename(s3_key)
                        
                        dataframes_info[s3_key] = {
                            "file_name": file_name,
                            "folder_path": folder_path,
                            "columns": columns
                        }
        
        # Sauvegarder en JSON
        json_output_file = os.path.join("AI", "s3_dataframes_structure.json")
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(dataframes_info, f, indent=2, ensure_ascii=False)
        print(f"\nInformations sauvegardées en JSON dans {json_output_file}")
        
        # Créer le fichier Markdown
        md_output_file = os.path.join("AI", "s3_dataframes_structure.md")
        
        with open(md_output_file, 'w', encoding='utf-8') as f:
            f.write("# Structure des DataFrames dans S3\n\n")
            f.write("Ce document répertorie tous les DataFrames du bucket S3 et leurs colonnes respectives.\n\n")
            
            # Organiser par dossier
            folders = {}
            for s3_key, info in dataframes_info.items():
                folder = info['folder_path'] or 'Root'
                if folder not in folders:
                    folders[folder] = []
                folders[folder].append((info['file_name'], info['columns']))
            
            # Écrire le contenu organisé
            for folder, files in sorted(folders.items()):
                f.write(f"## {folder}\n")
                for file_name, columns in sorted(files):
                    f.write(f"### {file_name}\n")
                    f.write("Colonnes :\n")
                    for col in columns:
                        f.write(f"- {col}\n")
                    f.write("\n")
        
        print(f"Informations sauvegardées en Markdown dans {md_output_file}")
        print(f"Nombre total de DataFrames traités : {len(dataframes_info)}")
        
    except Exception as e:
        print(f"Erreur lors de l'accès au bucket S3: {str(e)}")

if __name__ == "__main__":
    main()
