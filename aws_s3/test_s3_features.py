from connect_s3 import S3Manager
import json
import os

def test_all_features():
    """Test toutes les fonctionnalités de S3Manager"""
    s3 = S3Manager()
    
    print("\n1. Test de création et upload de fichiers de test")
    # Créer un fichier texte de test
    with open("test_text.txt", "w") as f:
        f.write("Ceci est un test de contenu")
    
    # Créer des données JSON de test
    test_data = {
        "name": "test_item",
        "values": [1, 2, 3, 4, 5],
        "metadata": {
            "type": "test",
            "version": "1.0"
        }
    }
    
    # Upload des fichiers de test
    s3.upload_file("test_text.txt", "tests/test_text.txt")
    s3.upload_json(test_data, "tests/test_data.json")
    
    print("\n2. Test de listage des fichiers")
    print("\nTous les fichiers:")
    all_files = s3.list_files()
    for file in all_files:
        print(f"- {file}")
    
    print("\nFichiers dans le dossier 'tests/':")
    test_files = s3.list_files("tests/")
    for file in test_files:
        print(f"- {file}")
    
    print("\n3. Test de vérification d'existence et taille")
    test_file = "tests/test_text.txt"
    if s3.file_exists(test_file):
        size = s3.get_file_size(test_file)
        print(f"Le fichier {test_file} existe et fait {size} bytes")
    else:
        print(f"Le fichier {test_file} n'existe pas")
    
    print("\n4. Test de téléchargement")
    if s3.file_exists("tests/test_data.json"):
        s3.download_file("tests/test_data.json", "downloaded_test.json")
        # Vérifier le contenu
        with open("downloaded_test.json", "r") as f:
            downloaded_data = json.load(f)
        print("Contenu du fichier JSON téléchargé:", json.dumps(downloaded_data, indent=2))
    
    print("\n5. Structure des dossiers dans S3")
    def print_s3_structure(prefix="", indent="", max_depth=3, current_depth=0):
        """Affiche la structure des dossiers et fichiers dans S3 avec une profondeur limitée"""
        if current_depth >= max_depth:
            print(f"{indent}  ...")
            return
            
        files = s3.list_files(prefix)
        folders = set()
        current_files = []
        
        # Collecter les dossiers et fichiers
        for file in files:
            relative_path = file[len(prefix):] if prefix else file
            if "/" in relative_path:
                folder = relative_path.split("/")[0]
                folders.add(folder)
            else:
                current_files.append(file)
        
        # Afficher les fichiers du dossier courant
        for file in current_files:
            print(f"{indent}📄 {os.path.basename(file)}")
        
        # Afficher et parcourir les sous-dossiers
        for folder in sorted(folders):
            print(f"{indent}📁 {folder}/")
            new_prefix = f"{prefix}{folder}/" if prefix else f"{folder}/"
            print_s3_structure(new_prefix, indent + "  ", max_depth, current_depth + 1)
    
    print("\nStructure du bucket S3:")
    print_s3_structure()
    
    print("\n6. Test de suppression")
    print("Suppression des fichiers de test...")
    s3.delete_file("tests/test_text.txt")
    s3.delete_file("tests/test_data.json")
    
    # Nettoyage des fichiers locaux
    if os.path.exists("test_text.txt"):
        os.remove("test_text.txt")
    if os.path.exists("downloaded_test.json"):
        os.remove("downloaded_test.json")
    
    print("\nTest terminé !")

if __name__ == "__main__":
    test_all_features()
