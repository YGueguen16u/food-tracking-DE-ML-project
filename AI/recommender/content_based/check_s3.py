"""
Script de diagnostic pour vérifier le contenu de S3
"""
from aws_s3.connect_s3 import S3Manager

def list_bucket_contents():
    """Liste tout le contenu du bucket S3"""
    s3_manager = S3Manager()
    
    print(f"\nBucket name: {s3_manager.bucket_name}")
    print("\nContenu du bucket:")
    try:
        response = s3_manager.s3_client.list_objects_v2(Bucket=s3_manager.bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"- {obj['Key']}")
        else:
            print("Le bucket est vide")
    except Exception as e:
        print(f"Erreur lors de la lecture du bucket: {str(e)}")
    
    # Vérifier spécifiquement les fichiers qui nous intéressent
    files_to_check = [
        'AI/recommender/content_based/results/stats.json',
        'AI/recommender/content_based/results/recommendations.json'
    ]
    
    print("\nVérification des fichiers spécifiques:")
    for file_key in files_to_check:
        try:
            s3_manager.s3_client.head_object(Bucket=s3_manager.bucket_name, Key=file_key)
            print(f"✓ {file_key} existe")
            
            # Lire le contenu du fichier
            response = s3_manager.s3_client.get_object(Bucket=s3_manager.bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            print(f"Taille du fichier: {len(content)} caractères")
            
        except s3_manager.s3_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404' or error_code == 'NoSuchKey':
                print(f"✗ {file_key} n'existe pas")
            else:
                print(f"! Erreur lors de la vérification de {file_key}: {str(e)}")

if __name__ == "__main__":
    list_bucket_contents()
