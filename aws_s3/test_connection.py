from connect_s3 import S3Manager
import sys
import os

def test_s3_connection():
    try:
        # Créer une instance de S3Manager
        s3 = S3Manager()
        
        # Tester la connexion en listant les fichiers
        files = s3.list_files()
        
        print("\nConnexion réussie ! ")
        print("\nFichiers dans le bucket :")
        if files:
            for file in files:
                print(f"- {file}")
        else:
            print("Le bucket est vide")
        
        # Test d'upload du fichier CSV
        current_dir = os.path.dirname(os.path.abspath(__file__))
        test_file = os.path.join(current_dir, "test_data.csv")
        if os.path.exists(test_file):
            print(f"\nEnvoi du fichier {test_file} vers S3...")
            s3.upload_file(test_file, "test/test_data.csv")
            print(f"Fichier test_data.csv envoyé avec succès ! ")
        else:
            print(f"\nFichier {test_file} non trouvé !")
            
    except Exception as e:
        print("\nErreur ! ")
        print(f"Détails de l'erreur : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    print("\nTest de connexion au bucket S3...")
    test_s3_connection()
