# data_engineering/Extract_data/smart_extract.py

"""
Smart Data Extraction Module for Food Tracking Project

This module provides a smart data extraction script that:
    - Determines the latest date in S3
    - Starts extraction from the next month
    - Handles progress tracking and logging
    - Includes error handling and logging
    
Typical usage:
    python smart_extract.py

The script will output the extraction progress to the console and exit with
status code 1 if any errors occur.  
"""
import sys
import os
from datetime import datetime
import subprocess
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from aws_s3.connect_s3 import S3Manager

def get_latest_date_from_s3():
    """
    Gets the latest date from S3 (in YYYY_MM format) if available

    Returns:
        tuple: (year, month) if a file is found, None otherwise
    """

    try:
        s3 = S3Manager()
        files = s3.list_files("raw_data_ingestion/")
        
        if not files:
            print("No files found in S3. Starting from the beginning...")
            return None
            
        # Extract the dates (YYYY_MM) from the file names
        dates = []
        for file in files:
            try:
                # The file name is the last element after '/'
                filename = file.split('/')[-1]
                year, month = map(int, filename.split('_')[:2])
                dates.append((year, month))
            except (ValueError, IndexError):
                continue
        
        if not dates:
            return None
            
        # Find the latest date
        latest_date = max(dates)
        print(f"Latest date found: {latest_date[0]}-{latest_date[1]:02d}")
        return latest_date
        
    except Exception as e:
        print(f"Error while searching for the latest date: {str(e)}")
        return None

def get_start_date():
    """
    Determine the starting date for data extraction.
    
    Returns:
        str: Start date in YYYY-MM-DD format
        
    The function either:
    - Uses the month following the latest date found in S3
    - Or starts one year before current date if no data exists
    """
    latest_date = get_latest_date_from_s3()
    
    if latest_date is None:
        # If no date found, start one year ago
        start_date = datetime.now() - relativedelta(years=1)
        print(f"No date found. Starting from: {start_date.strftime('%Y-%m-%d')}")
    else:
        # Take the first day of the next month
        year, month = latest_date
        start_date = datetime(year, month, 1) + relativedelta(months=1)
        print(f"Resuming extraction from: {start_date.strftime('%Y-%m-%d')}")
    
    return start_date.strftime("%Y-%m-%d")

def run_extract_data(start_date):
    """
    Run the extract_data.py script with the given start date.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        
    Raises:
        SystemExit: If the extract_data.py script fails
    """
    script_path = os.path.join(os.path.dirname(__file__), 'extract_data.py')
    print(f"Starting extraction from {start_date}...")
    
    try:
        subprocess.run([sys.executable, script_path, start_date], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running extract_data.py: {str(e)}")
        sys.exit(1)

def main():
    """
    Main function for smart data extraction.
    
    This function:
    1. Gets the appropriate start date
    2. Runs the data extraction process
    """
    start_date = get_start_date()
    run_extract_data(start_date)

if __name__ == "__main__":
    main()
