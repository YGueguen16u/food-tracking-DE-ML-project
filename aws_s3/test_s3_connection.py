from connect_s3 import S3Manager

def test_s3_connection():
    try:
        # Initialize S3Manager
        s3_manager = S3Manager()
        
        # Test listing files
        print("Testing S3 connection by listing files...")
        files = s3_manager.list_files()
        print("Successfully connected to S3!")
        print(f"Files in bucket: {files}")
        
        return True
    except Exception as e:
        print(f"Error connecting to S3: {str(e)}")
        return False

if __name__ == "__main__":
    test_s3_connection()
