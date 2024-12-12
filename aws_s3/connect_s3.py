import os
import configparser
import boto3
from botocore.exceptions import ClientError


class S3Manager:
    """Manages interactions with AWS S3 bucket."""
    
    def __init__(self, config_file='s3_config.txt'):
        """
        Initialize S3 client using configuration file.
        
        Args:
            config_file (str): Path to configuration file (relative to this script)
        """
        # Read configuration
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), config_file)
        config.read(config_path)
        
        # Set up AWS credentials
        self.aws_access_key_id = config['aws_credentials']['aws_access_key_id']
        self.aws_secret_access_key = config['aws_credentials']['aws_secret_access_key']
        self.region = config['aws_credentials']['region']
        self.bucket_name = config['s3_config']['bucket_name']
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region
        )

    def upload_file(self, file_path, s3_key=None):
        """
        Upload a file to S3 bucket.
        
        Args:
            file_path (str): Local path to the file
            s3_key (str, optional): The key to use in S3. If None, uses filename
        
        Returns:
            bool: True if file was uploaded, else False
        """
        if s3_key is None:
            s3_key = os.path.basename(file_path)
            
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            print(f"Successfully uploaded {file_path} to {self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            print(f"Error uploading file to S3: {str(e)}")
            return False

    def download_file(self, s3_key, local_path):
        """
        Download a file from S3 bucket.
        
        Args:
            s3_key (str): The key of the file in S3
            local_path (str): Local path where to save the file
        
        Returns:
            bool: True if file was downloaded, else False
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"Successfully downloaded {s3_key} to {local_path}")
            return True
        except ClientError as e:
            print(f"Error downloading file from S3: {str(e)}")
            return False

    def list_files(self, prefix=""):
        """
        List files in the S3 bucket.
        
        Args:
            prefix (str): Only list objects beginning with prefix
        
        Returns:
            list: List of object keys in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            print(f"Error listing files in S3: {str(e)}")
            return []