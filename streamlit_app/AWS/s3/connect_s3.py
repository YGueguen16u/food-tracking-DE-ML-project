import os
import boto3
from botocore.exceptions import ClientError
import json
import logging
from st_files_connection import FilesConnection
import pandas as pd

# Configure logging
logger = logging.getLogger("AWS.S3Manager")
logger.setLevel(logging.WARNING)  # Set logging level to WARNING to reduce noise


class S3Manager:
    """Manages interactions with AWS S3 bucket using Streamlit FilesConnection."""

    def __init__(self):
        """
        Initialize S3 connection using Streamlit FilesConnection.
        """
        # Create connection object
        self.conn = FilesConnection('s3')

    def load_data(self, s3_key):
        """
        Load data from S3 bucket.

        Args:
            s3_key (str): The key of the file in S3.

        Returns:
            pd.DataFrame: DataFrame containing the loaded data.
        """
        return self.conn.read(s3_key, input_format="csv")

    def upload_file(self, file_path, s3_key=None):
        """
        Upload a file to the S3 bucket.

        Args:
            file_path (str): Local path to the file.
            s3_key (str, optional): The key to use in S3. If None, uses the filename.

        Returns:
            bool: True if the file was uploaded successfully, False otherwise.
        """
        if s3_key is None:
            s3_key = os.path.basename(file_path)

        try:
            self.conn.write(file_path, s3_key)
            logger.info(f"Uploaded {file_path} to S3 as {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            return False

    def download_file(self, s3_key, local_path):
        """
        Download a file from the S3 bucket.

        Args:
            s3_key (str): The key of the file in S3.
            local_path (str): Local path to save the file.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        try:
            self.conn.download(s3_key, local_path)
            logger.info(f"Downloaded {s3_key} from S3 to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading file from S3: {e}")
            return False

    def list_files(self, prefix=""):
        """
        List files in the S3 bucket with a specific prefix.

        Args:
            prefix (str): Only list objects starting with this prefix.

        Returns:
            list: A list of object keys in the bucket.
        """
        try:
            keys = self.conn.list(prefix)
            logger.info(f"Listed {len(keys)} files in bucket with prefix '{prefix}'")
            return keys
        except Exception as e:
            logger.error(f"Error listing files in S3: {e}")
            return []

    def delete_file(self, s3_key):
        """
        Delete a file from the S3 bucket.

        Args:
            s3_key (str): The key of the file to delete.

        Returns:
            bool: True if the file was deleted successfully, False otherwise.
        """
        try:
            self.conn.delete(s3_key)
            logger.info(f"Deleted {s3_key} from bucket")
            return True
        except Exception as e:
            logger.error(f"Error deleting file from S3: {e}")
            return False

    def file_exists(self, s3_key):
        """
        Check if a file exists in the S3 bucket.

        Args:
            s3_key (str): The key of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            return self.conn.exists(s3_key)
        except Exception as e:
            logger.error(f"Error checking existence of {s3_key}: {e}")
            return False

    def upload_json(self, data, s3_key):
        """
        Upload JSON data directly to the S3 bucket.

        Args:
            data (dict): The data to serialize to JSON.
            s3_key (str): The key to use in S3.

        Returns:
            bool: True if the JSON data was uploaded successfully, False otherwise.
        """
        try:
            json_data = json.dumps(data)
            self.conn.write(json_data, s3_key)
            logger.info(f"Uploaded JSON to {s3_key} in bucket")
            return True
        except (Exception, TypeError) as e:
            logger.error(f"Error uploading JSON to S3: {e}")
            return False

    def save_ai_results(self, model_type, results, filename=None, timestamp=None):
        """
        Save AI model results to specific S3 folders.

        Args:
            model_type (str): Type of AI model (e.g., 'clustering', 'recommender')
            results: Results to save (can be dict, DataFrame, or other serializable object)
            filename (str, optional): Custom filename. If None, will be auto-generated
            timestamp (str, optional): Timestamp to use in filename. If None, current time used

        Returns:
            str: S3 key where results were saved
        """
        import datetime

        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        # Generate filename if not provided
        if filename is None:
            filename = f"{model_type}_results_{timestamp}.json"

        # Construct S3 key
        s3_key = f"AI/{model_type}/{filename}"

        # Convert results to appropriate format
        if isinstance(results, pd.DataFrame):
            # For DataFrames, save as CSV
            local_path = f"/tmp/{filename}.csv"
            results.to_csv(local_path, index=False)
        else:
            # For other objects, save as JSON
            local_path = f"/tmp/{filename}"
            with open(local_path, 'w') as f:
                json.dump(results, f)

        # Upload to S3
        success = self.upload_file(local_path, s3_key)

        # Clean up temporary file
        if os.path.exists(local_path):
            os.remove(local_path)

        if success:
            return s3_key
        return None

    def upload_with_overwrite(self, file_path, s3_key):
        """
        Upload a file to S3 bucket, overwriting if it already exists.

        Args:
            file_path (str): Local path to the file
            s3_key (str): The key to use in S3

        Returns:
            bool: True if file was uploaded, else False
        """
        try:
            # Vérifier si le fichier existe
            try:
                self.conn.exists(s3_key)
            except Exception as e:
                if e.response['Error']['Code'] == '404':
                    pass
                else:
                    raise e

            # Upload le fichier (écrase si existe)
            self.conn.write(file_path, s3_key)
            return True

        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}")
            return False