# AI/anomaly_detection/utils/data_loader.py

"""
Data loader for anomaly detection.
"""
import pandas as pd
import io
from aws_s3.connect_s3 import S3Manager

class DataLoader:
    """
    Load data from S3 for anomaly detection
    """
    
    def __init__(self):
        """Initialize S3 connection"""
        self.s3_manager = S3Manager()
        
    def load_dataframe_from_s3(self, s3_key):
        """
        Load a DataFrame from S3
        
        Args:
            s3_key (str): Path to the file in S3
            
        Returns:
            pd.DataFrame: DataFrame loaded
        """
        try:
            response = self.s3_manager.s3_client.get_object( 
                Bucket=self.s3_manager.bucket_name, 
                Key=s3_key
            ) # Get the object from S3
            return pd.read_excel(io.BytesIO(response['Body'].read())) # Load the object as a DataFrame
        except Exception as e:
            print(f"Error loading {s3_key}: {str(e)}")
            return None
    
    def load_training_data(self):
        """
        Load training data from S3
        
        Returns:
            tuple: (filtered data, daily means, food proportions)
        """
        # Load filtered data
        filtered_data = self.load_dataframe_from_s3(
            'transform/folder_2_filter_data/combined_meal_data_filtered.xlsx'
        )
        
        # Load daily means
        daily_stats = self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/daily/daily_window_function_duckdb.xlsx'
        )
        
        # Load food proportions
        food_proportions = self.load_dataframe_from_s3(
            'transform/folder_4_windows_function/type_food/user_food_proportion_duckdb.xlsx'
        )
        
        return filtered_data, daily_stats, food_proportions
    
    def get_reference_data(self):
        """
        Load reference data (food)
        
        Returns:
            pd.DataFrame: Reference food data
        """
        return self.load_dataframe_from_s3('reference_data/food/food_processed.xlsx')
