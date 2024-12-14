"""
This script converts Excel files from multiple folders to Parquet format.
It preserves the folder structure while converting the files.
"""

import os
import sys
import pandas as pd

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(project_root)

from aws_s3.connect_s3 import S3Manager


class ExcelToParquet:
    """
    Class to convert Excel files to Parquet format.
    Handles files from multiple folders while preserving structure.
    """

    def __init__(self):
        """Initialize S3 connection and define source folders."""
        self.s3 = S3Manager()
        self.source_folders = [
            "transform/folder_2_filter_data",
            "transform/folder_3_group_data",
            "transform/folder_4_windows_function",
            "transform/folder_5_percentage_change"
        ]

    def get_parquet_path(self, original_path):
        """
        Convert original S3 path to parquet path in folder_6_parquet.
        
        Args:
            original_path (str): Original S3 path
            
        Returns:
            str: Path in the folder_6_parquet with _filtered suffix
        """
        # Extract the folder name (e.g., "folder_2_filter_data")
        parts = original_path.split('/')
        folder_name = parts[1]  # Get the folder_X part
        file_name = parts[-1]   # Get the file name
        
        # Create new path in folder_6_parquet with _filtered suffix
        new_path = f"transform/folder_6_parquet/{folder_name}_filtered/{file_name.replace('.xlsx', '.parquet')}"
        return new_path

    def convert_file(self, excel_path, parquet_path):
        """
        Convert a single Excel file to Parquet format.
        If the Excel file has multiple sheets, creates separate parquet files.
        
        Args:
            excel_path (str): S3 path to the Excel file
            parquet_path (str): S3 path for the output Parquet file
        """
        temp_excel = "temp_excel.xlsx"
        
        try:
            # Download Excel file
            if not self.s3.download_file(excel_path, temp_excel):
                print(f"Failed to download: {excel_path}")
                return False

            # Read all sheets
            sheet_names = []
            with pd.ExcelFile(temp_excel) as excel_file:
                sheet_names = excel_file.sheet_names

            for sheet_name in sheet_names:
                # Create parquet path for this sheet
                if len(sheet_names) > 1:
                    # If multiple sheets, add sheet name to parquet file name
                    sheet_parquet_path = parquet_path.replace('.parquet', f'_{sheet_name}.parquet')
                else:
                    sheet_parquet_path = parquet_path

                # Read the specific sheet
                df = pd.read_excel(temp_excel, sheet_name=sheet_name)
                
                # Create temporary parquet file for this sheet
                temp_parquet = f"temp_parquet_{sheet_name}.parquet"
                
                try:
                    # Write to Parquet
                    df.to_parquet(temp_parquet, index=False)
                    
                    # Upload Parquet file
                    if not self.s3.upload_file(temp_parquet, sheet_parquet_path):
                        print(f"Failed to upload sheet {sheet_name} to: {sheet_parquet_path}")
                        continue
                        
                    print(f"Successfully converted sheet {sheet_name}: {excel_path} -> {sheet_parquet_path}")
                
                finally:
                    # Cleanup temporary parquet file
                    if os.path.exists(temp_parquet):
                        try:
                            os.remove(temp_parquet)
                        except:
                            pass
            
            return True
            
        finally:
            # Cleanup temporary excel file
            if os.path.exists(temp_excel):
                try:
                    os.remove(temp_excel)
                except:
                    pass

    def process_folder(self, folder_path):
        """
        Convert all Excel files in a folder to Parquet format.
        
        Args:
            folder_path (str): S3 folder path to process
        """
        # List all files in the folder
        files = self.s3.list_files(folder_path)
        
        for file in files:
            if file.endswith('.xlsx'):
                # Create corresponding parquet path in folder_6_parquet
                parquet_path = self.get_parquet_path(file)
                
                # Convert the file
                self.convert_file(file, parquet_path)

    def convert_all_folders(self):
        """Convert all Excel files in all source folders to Parquet format."""
        for folder in self.source_folders:
            print(f"\nProcessing folder: {folder}")
            self.process_folder(folder)


def main():
    """
    Main function to convert data to parquet format
    """
    try:
        print("Converting data to parquet format...")
        
        # Initialize converter
        converter = ExcelToParquet()
        
        # Convert all folders
        print("Converting all folders...")
        converter.convert_all_folders()
        
        print("Data converted to parquet successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()