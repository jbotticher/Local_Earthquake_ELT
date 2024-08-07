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
CURRENT_PREFIX = 'earthquake_data/'
HISTORICAL_PREFIX = 'earthquake_data_hist/'
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

def move_old_files(s3_client):
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=CURRENT_PREFIX)
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.startswith(CURRENT_PREFIX):
                    new_key = key.replace(CURRENT_PREFIX, HISTORICAL_PREFIX, 1)
                    s3_client.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=new_key)
                    s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
                    logger.info(f"Moved {key} to {new_key}")
        else:
            logger.info("No files found in earthquake_data/")
    except ClientError as e:
        logger.error(f"Client error while moving files in S3: {e}")
        raise

def main():
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    while True:
        try:
            # Calculate the date range for yesterday
            end_time = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            start_time = end_time - datetime.timedelta(days=1)

            # Format dates
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            filename = f"{start_time.strftime('%Y-%m-%d')}.json"
            s3_key = f'{CURRENT_PREFIX}/{filename}'

            # Move old files to the historical folder
            move_old_files(s3_client)

            # Fetch data and upload to S3
            data = fetch_earthquake_data(start_time_str, end_time_str)
            upload_to_s3(data, s3_key)

            # Log success message and wait before the next iteration
            logger.info("Job successful, waiting 60 seconds until next run.")
            time.sleep(60)
        
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            # Optional: sleep for a longer period before retrying to avoid rapid retries in case of persistent errors
            time.sleep(60)

if __name__ == "__main__":
    main()
