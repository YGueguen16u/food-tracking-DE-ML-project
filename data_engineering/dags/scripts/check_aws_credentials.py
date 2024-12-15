import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

required_vars = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_DEFAULT_REGION',
    'AWS_BUCKET_NAME'
]

def check_aws_credentials():
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            logger.error(f"Missing environment variable: {var}")
        else:
            # Log only the first few characters of sensitive credentials
            if 'KEY' in var:
                logger.info(f"{var} is set (starts with: {value[:4]}...)")
            else:
                logger.info(f"{var} is set to: {value}")
    
    return len(missing_vars) == 0

if __name__ == "__main__":
    if check_aws_credentials():
        logger.info("All required AWS credentials are set!")
    else:
        logger.error("Some required AWS credentials are missing!")
