import requests
import boto3
import datetime
import json
import logging
import time
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
USGS_URL = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
S3_BUCKET = 'dec-earthquake-bucket'
S3_PREFIX = 'earthquake_data'
AWS_REGION = 'us-east-2'

def fetch_earthquake_data(start_time_str, end_time_str):
    try:
        params = {
            'format': 'geojson',
            'starttime': start_time_str,
            'endtime': end_time_str,
        }
        response = requests.get(USGS_URL, params=params, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from USGS API: {e}")
        raise

def upload_to_s3(data, s3_key):
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(data))
        logger.info(f"Data successfully uploaded to s3://{S3_BUCKET}/{s3_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"Credentials error while accessing S3: {e}")
        raise
    except ClientError as e:
        logger.error(f"Client error while uploading to S3: {e}")
        raise

def main():
    while True:
        try:
            # Calculate the date range for the last 2 days
            end_time = datetime.datetime.utcnow()
            start_time = end_time - datetime.timedelta(days=2)

            # Format dates
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            filename = end_time.strftime('%Y-%m-%d') + '.json'
            s3_key = f'{S3_PREFIX}/{filename}'

            # Fetch data and upload to S3
            data = fetch_earthquake_data(start_time_str, end_time_str)
            upload_to_s3(data, s3_key)

            # Wait for 5 seconds before the next iteration
            time.sleep(5)
        
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            # Optional: sleep for a longer period before retrying to avoid rapid retries in case of persistent errors
            time.sleep(60)

if __name__ == "__main__":
    main()
