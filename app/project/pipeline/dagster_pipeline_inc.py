import os
import datetime
import time
import logging
import pytz
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient

# Configure Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure environmental variables
load_dotenv()

def fetch_earthquake_data(start_time_str: str, end_time_str: str) -> dict:
    logger.info(f"Fetching earthquake data from USGS API for period: {start_time_str} to {end_time_str}")
    try:
        client = USGSClient(os.getenv('USGS_URL'))
        data = client.fetch_data(start_time_str, end_time_str)
        logger.info(f"Fetched {len(data)} records from USGS API")
        return data
    except Exception as e:
        logger.error(f"Error fetching earthquake data: {e}")
        raise

def upload_to_s3(data: dict, filename: str) -> None:
    logger.info(f"Uploading data to S3 - Filename: {filename}")
    try:
        s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
        s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
        s3_client.upload_to_s3(data, s3_key)
        logger.info(f"Data successfully uploaded to s3://{os.getenv('S3_BUCKET')}/{s3_key}")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")
        raise

def trigger_sync() -> None:
    logger.info(f"Triggering Airbyte sync for connection ID: {os.getenv('AIRBYTE_CONNECTION_ID')}")
    try:
        airbyte_client = AirbyteClient(
            server_name=os.getenv('AIRBYTE_SERVER_NAME'),
            username=os.getenv('AIRBYTE_USERNAME'),
            password=os.getenv('AIRBYTE_PASSWORD')
        )
        job_id = airbyte_client.trigger_sync(os.getenv('AIRBYTE_CONNECTION_ID'))

        # Check the status of the sync job using the method from the AirbyteClient instance
        logger.info(f"Checking status of Airbyte sync job with Job ID: {job_id}")
        while True:
            status = airbyte_client.check_job_status(job_id)
            if status == "succeeded":
                logger.info(f"Airbyte sync job {job_id} completed successfully.")
                return  # Successfully completed
            elif status == "failed":
                logger.error(f"Airbyte sync job {job_id} failed.")
                raise Exception(f"Airbyte sync job {job_id} failed.")
            else:
                logger.info(f"Job {job_id} is still in progress. Checking again in 10 seconds.")
                time.sleep(10)  # Poll every 10 seconds

    except Exception as e:
        logger.error(f"Error triggering or checking Airbyte sync: {e}")
        raise

def main():
    logger.info("Starting the pipeline execution")

    # Calculate start_time and end_time
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)  # Current date - 1 day
    end_time = datetime.datetime.utcnow()

    # Convert UTC time to EST
    est = pytz.timezone('US/Eastern')
    start_time_est = start_time.astimezone(est)
    end_time_est = end_time.astimezone(est)

    # Format dates in ISO 8601 format for USGS API
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Format dates in EST for the S3 file name
    start_time_est_str = start_time_est.strftime('%Y-%m-%dT%H-%M-%S')  # Example: 2023-08-09T07-30-00

    try:
        # Fetch earthquake data
        data = fetch_earthquake_data(start_time_str, end_time_str)

        # Upload data to S3
        filename = f"{start_time_est_str}.json"  # Use EST formatted date-time for the file name
        upload_to_s3(data, filename)

        # Trigger Airbyte sync
        trigger_sync()

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

    logger.info("Pipeline execution completed successfully")

if __name__ == "__main__":
    main()
