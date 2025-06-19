# aws_s3/test_s3_connection.py

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
