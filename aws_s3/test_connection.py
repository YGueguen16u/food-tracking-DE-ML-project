from connect_s3 import S3Manager
import sys

def test_s3_connection():
    try:
        # Créer une instance de S3Manager
        s3 = S3Manager()
        
        # Tester la connexion en listant les fichiers
        files = s3.list_files()
        
        print("\nConnexion réussie ! ✅")
        print("\nFichiers dans le bucket :")
        if files:
            for file in files:
                print(f"- {file}")
        else:
            print("Le bucket est vide")
            
    except Exception as e:
        print("\nErreur de connexion ! ❌")
        print(f"Détails de l'erreur : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    print("\nTest de connexion au bucket S3...")
    test_s3_connection()
