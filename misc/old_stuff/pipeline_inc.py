import datetime
import json
import logging
import time
import os
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient



def fetch_earthquake_data(start_time_str, end_time_str):
    client = USGSClient(USGS_URL)
    return client.fetch_data(start_time_str, end_time_str)

def main():
    s3_client = S3Client(S3_BUCKET, AWS_REGION)
    airbyte_client = AirbyteClient(
        server_name=AIRBYTE_SERVER_NAME, 
        username=AIRBYTE_USERNAME, 
        password=AIRBYTE_PASSWORD
    )
    
    while True:
        try:
            # Validate Airbyte connection
            if not airbyte_client.valid_connection():
                raise Exception("Invalid Airbyte connection")

            # Calculate the date range for yesterday
            end_time = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            start_time = end_time - datetime.timedelta(days=1)

            # Format dates
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            filename = f"{start_time.strftime('%Y-%m-%d')}.json"
            s3_key = f'{CURRENT_PREFIX}/{filename}'

            # Move old files to the historical folder
            s3_client.move_old_files(CURRENT_PREFIX, HISTORICAL_PREFIX)

            # Fetch data and upload to S3
            data = fetch_earthquake_data(start_time_str, end_time_str)
            s3_client.upload_to_s3(data, s3_key)

            # Trigger Airbyte sync
            airbyte_client.trigger_sync(connection_id=AIRBYTE_CONNECTION_ID)

            # Log success message and wait before the next iteration
            logger.info("Job successful, waiting 60 seconds until next run.")
            time.sleep(60)
        
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            # Optional: sleep for a longer period before retrying to avoid rapid retries in case of persistent errors
            time.sleep(60)

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Configuration
    USGS_URL = os.environ.get('USGS_URL')
    S3_BUCKET = os.environ.get('S3_BUCKET')
    CURRENT_PREFIX = os.environ.get('CURRENT_PREFIX')
    HISTORICAL_PREFIX = os.environ.get('HISTORICAL_PREFIX')
    AWS_REGION = os.environ.get('AWS_REGION')
    
    AIRBYTE_SERVER_NAME = os.environ.get('AIRBYTE_SERVER_NAME')
    AIRBYTE_USERNAME = os.environ.get('AIRBYTE_USERNAME')
    AIRBYTE_PASSWORD = os.environ.get('AIRBYTE_PASSWORD')
    AIRBYTE_CONNECTION_ID = os.environ.get('AIRBYTE_CONNECTION_ID')

    # Run the pipeline
    main()