# aws_s3/test_connection.py

"""
AWS S3 Connection Test Module

This module provides a simple test script to verify the AWS S3 connection
and basic functionality of the S3Manager class. It tests:
    - Connection to S3 bucket
    - Listing files in the bucket
    - File upload capability (if test file exists)

Typical usage:
    python test_connection.py

The script will output the test results to the console and exit with
status code 1 if any errors occur.
"""

from connect_s3 import S3Manager
import sys
import os

def test_s3_connection():
    """
    Test the connection to AWS S3 and basic S3Manager functionality.
    
    This function attempts to:
        1. Create an S3Manager instance
        2. List files in the bucket
        3. Upload a test file if available
    
    Returns:
        None: Results are printed to stdout
    
    Raises:
        SystemExit: If any error occurs during testing
    """
    try:
        # Create an instance of S3Manager
        s3 = S3Manager()
        
        # Test the connection by listing files
        files = s3.list_files()
        
        print("\nConnection successful!")
        print("\nFiles in bucket:")
        if files:
            for file in files:
                print(f"- {file}")
        else:
            print("The bucket is empty")
        
        # Test file upload
        current_dir = os.path.dirname(os.path.abspath(__file__))
        test_file = os.path.join(current_dir, "test_data.csv")
        if os.path.exists(test_file):
            print(f"\nSending file {test_file} to S3...")
            s3.upload_file(test_file, "test/test_data.csv")
            print(f"File test_data.csv successfully uploaded!")
        else:
            print(f"\nFile {test_file} not found!")
            
    except Exception as e:
        print("\nError!")
        print(f"Error details: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    print("\nTesting connection to S3 bucket...")
    test_s3_connection()
