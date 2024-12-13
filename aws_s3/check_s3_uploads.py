from connect_s3 import S3Manager

def check_s3_content():
    s3 = S3Manager()
    files = s3.list_files("raw_data_ingestion/")
    print("\nFichiers dans S3 :")
    for file in files:
        print(f"- {file}")

if __name__ == "__main__":
    check_s3_content()
