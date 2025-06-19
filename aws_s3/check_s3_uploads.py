# aws_s3/check_s3_uploads.py

"""
Script to check and display the contents of the S3 bucket.
This utility script lists all files in the raw_data_ingestion/ directory of the S3 bucket,
helping to monitor data ingestion and verify successful uploads.
"""

from connect_s3 import S3Manager

def check_s3_content():
    """
    List all files in the raw_data_ingestion/ directory of the S3 bucket.
    
    This function connects to S3 using the S3Manager class and retrieves
    a list of all files in the raw_data_ingestion/ directory. It then
    prints each file name to the console.
    
    Returns:
        None: Results are printed to stdout
    """
    s3 = S3Manager()
    files = s3.list_files("raw_data_ingestion/")
    print("\nFiles in S3 :")
    for file in files:
        print(f"- {file}")

if __name__ == "__main__":
    check_s3_content()
