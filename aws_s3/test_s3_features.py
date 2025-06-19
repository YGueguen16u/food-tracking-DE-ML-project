# aws_s3/test_s3_features.py

"""
AWS S3 Features Test Module

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
import json
import os

def test_all_features():
    """
    Test all features of S3Manager.
    
    This function attempts to:
        1. Create an S3Manager instance
        2. List files in the bucket
        3. Upload a test file if available
    
    Returns:
        None: Results are printed to stdout
    
    Raises:
        SystemExit: If any error occurs during testing
    """
    s3 = S3Manager()
    
    print("\n1. Test of file creation and upload")
    # Create a test text file
    with open("test_text.txt", "w") as f:
        f.write("This is a test content")
    
    # Create test JSON data
    test_data = {
        "name": "test_item",
        "values": [1, 2, 3, 4, 5],
        "metadata": {
            "type": "test",
            "version": "1.0"
        }
    }
    
    # Upload test files
    s3.upload_file("test_text.txt", "tests/test_text.txt")
    s3.upload_json(test_data, "tests/test_data.json")
    
    print("\n2. Test of file listing")
    print("\nAll files:")
    all_files = s3.list_files()
    for file in all_files:
        print(f"- {file}")
    
    print("\nFiles in the 'tests/' folder:")
    test_files = s3.list_files("tests/")
    for file in test_files:
        print(f"- {file}")
    
    print("\n3. Test of file existence and size")
    test_file = "tests/test_text.txt"
    if s3.file_exists(test_file):
        size = s3.get_file_size(test_file)
        print(f"The file {test_file} exists and is {size} bytes")
    else:
        print(f"The file {test_file} does not exist")
    
    print("\n4. Test of file download")
    if s3.file_exists("tests/test_data.json"):
        s3.download_file("tests/test_data.json", "downloaded_test.json")
        # Check the content
        with open("downloaded_test.json", "r") as f:
            downloaded_data = json.load(f)
        print("Content of downloaded JSON file:", json.dumps(downloaded_data, indent=2))
    
    print("\n5. Test of S3 bucket structure")
    def print_s3_structure(prefix="", indent="", max_depth=3, current_depth=0):
        """
        Print the structure of folders and files in S3 with a limited depth
        
        Args:
            prefix (str): Only list objects beginning with prefix
            indent (str): Indentation for printing
            max_depth (int): Maximum depth to print
            current_depth (int): Current depth
            
        """
        if current_depth >= max_depth:
            print(f"{indent}  ...")
            return
            
        files = s3.list_files(prefix)
        folders = set()
        current_files = []
        
        # Collect folders and files
        for file in files:
            relative_path = file[len(prefix):] if prefix else file
            if "/" in relative_path:
                folder = relative_path.split("/")[0]
                folders.add(folder)
            else:
                current_files.append(file)
        
        # Print files in the current folder
        for file in current_files:
            print(f"{indent}📄 {os.path.basename(file)}")
        
        # Print and traverse subfolders
        for folder in sorted(folders):
            print(f"{indent}📁 {folder}/")
            new_prefix = f"{prefix}{folder}/" if prefix else f"{folder}/"
            print_s3_structure(new_prefix, indent + "  ", max_depth, current_depth + 1)
    
    print("\nS3 bucket structure:")
    print_s3_structure()
    
    print("\n6. Test of file deletion")
    print("Deleting test files...")
    s3.delete_file("tests/test_text.txt")
    s3.delete_file("tests/test_data.json")
    
    # Clean up local files
    if os.path.exists("test_text.txt"):
        os.remove("test_text.txt")
    if os.path.exists("downloaded_test.json"):
        os.remove("downloaded_test.json")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_all_features()
