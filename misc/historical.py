import datetime
import json
import logging
import os
import pytz
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import time
import base64
from dateutil.relativedelta import relativedelta

# Load environment variables
load_dotenv()

# Configuration from environment variables
USGS_URL = os.environ.get('USGS_URL')
S3_BUCKET = os.environ.get('S3_BUCKET')
AWS_REGION = os.environ.get('AWS_REGION')
AIRBYTE_SERVER_NAME = os.environ.get('AIRBYTE_SERVER_NAME')
AIRBYTE_CONNECTION_ID = os.environ.get('AIRBYTE_CONNECTION_ID')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class USGSClient:
    def __init__(self, url):
        self.url = url

    def fetch_data(self, start_time_str, end_time_str):
        try:
            params = {
                'format': 'geojson',
                'start': start_time_str,
                'end': end_time_str,
            }
            logger.info(f"Requesting data from USGS API with parameters: {params}")
            response = requests.get(self.url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from USGS API: {e}")
            raise

class S3Client:
    def __init__(self, bucket_name, region_name):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name

    def upload_to_s3(self, data, s3_key):
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=json.dumps(data))
            logger.info(f"Data successfully uploaded to s3://{self.bucket_name}/{s3_key}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Credentials error while accessing S3: {e}")
            raise
        except ClientError as e:
            logger.error(f"Client error while uploading to S3: {e}")
            raise

class AirbyteClient:
    def __init__(self, server_name: str, username: str, password: str):
        self.server_name = server_name
        self.username = username
        self.password = password
        self.token = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()
        self.headers = {"Authorization": f"Basic {self.token}"}
        logging.basicConfig(level=logging.INFO)

    def valid_connection(self) -> bool:
        """Check if connection is valid"""
        url = f"http://{self.server_name}:8001/api/public/v1/health"
        logging.info(f"Checking Airbyte server health at {url}")

        try:
            response = requests.get(url=url, headers=self.headers)
            if response.status_code == 200:
                logging.info("Airbyte connection is valid.")
                return True
            else:
                logging.error(f"Airbyte connection is not valid. Status code: {response.status_code}. Error message: {response.text}")
                raise Exception(
                    f"Airbyte connection is not valid. Status code: {response.status_code}. Error message: {response.text}"
                )
        except requests.RequestException as e:
            logging.error(f"Exception occurred during health check: {str(e)}")
            raise Exception(f"Exception occurred during health check: {str(e)}")

    def trigger_sync(self, connection_id: str):
        """Trigger sync for a connection_id"""
        url = f"http://{self.server_name}:8001/api/public/v1/jobs"
        
        def check_job_status(job_id: str):
            job_status_url = f"http://{self.server_name}:8001/api/public/v1/jobs/{job_id}"
            job_response = requests.get(url=job_status_url, headers=self.headers)
            if job_response.status_code == 200:
                job_data = job_response.json()
                return job_data.get("status")
            else:
                logging.error(f"Failed to get job status. Status code: {job_response.status_code}. Error message: {job_response.text}")
                raise Exception(f"Failed to get job status. Status code: {job_response.status_code}. Error message: {response.text}")

        try:
            data = {"connectionId": connection_id, "jobType": "sync"}
            logging.info(f"Triggering Airbyte sync job with connection ID: {connection_id} at {url}")
            response = requests.post(url=url, json=data, headers=self.headers)
            if response.status_code != 200:
                logging.error(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")
                raise Exception(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")

            job_id = response.json().get("jobId")
            if not job_id:
                logging.error(f"No jobId returned in response. Response: {response.text}")
                raise Exception(f"No jobId returned in response. Response: {response.text}")

            logging.info(f"Sync job triggered successfully. Job ID: {job_id}")

            while True:
                sleep_seconds = 5
                logging.info(f"Job {job_id} is running. Checking job status again in {sleep_seconds} seconds.")
                time.sleep(sleep_seconds)

                job_status = check_job_status(job_id)
                
                if job_status == "failed":
                    logging.error(f"Job {job_id} has failed.")
                    raise Exception(f"Job {job_id} has failed.")
                elif job_status == "succeeded":
                    logging.info(f"Job {job_id} has succeeded.")
                    return  # Successfully completed

        except requests.RequestException as e:
            logging.error(f"Exception occurred while triggering sync job: {str(e)}")
            raise Exception(f"Exception occurred while triggering sync job: {str(e)}")

def main():
    # Initialize clients
    usgs_client = USGSClient(USGS_URL)
    s3_client = S3Client(S3_BUCKET, AWS_REGION)
    airbyte_client = AirbyteClient(
        server_name=AIRBYTE_SERVER_NAME,
        username=os.environ.get('AIRBYTE_USERNAME'),
        password=os.environ.get('AIRBYTE_PASSWORD')
    )

    # Start from January 2020
    current_date = datetime.datetime(2020, 1, 1, tzinfo=pytz.UTC)
    end_date = datetime.datetime.now(tz=pytz.UTC)

    while current_date < end_date:
        # Calculate the start and end times for the period
        start_time = current_date
        end_time = (start_time + relativedelta(months=1)).replace(day=1)  # Move to the start of the next month

        # Convert to EST
        est = pytz.timezone('US/Eastern')
        start_time_est = start_time.astimezone(est)
        end_time_est = end_time.astimezone(est)

        # Format dates for request
        start_time_str = start_time_est.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
        end_time_str = end_time_est.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

        logger.info(f"Start time: {start_time_str}, End time: {end_time_str}")

        # Format filename with timestamp in EST
        timestamp = start_time_est.strftime('%Y-%m')
        filename = f"earthquake_data_{timestamp}.json"

        # Try fetching data and uploading to S3
        data_fetched = False
        retry_interval = 5  # Time to wait before retrying in seconds

        for attempt in range(3):  # Try up to 3 times before skipping
            try:
                data = usgs_client.fetch_data(start_time_str, end_time_str)
                s3_client.upload_to_s3(data, filename)
                logger.info(f"Data for {timestamp} uploaded successfully.")
                data_fetched = True
                break  # Exit loop on successful fetch and upload

            except requests.HTTPError as e:
                if e.response.status_code == 400:
                    logger.warning(f"Received 400 Bad Request error. Attempt {attempt + 1} of 3.")
                    # Retry with a 15-day period if 400 error is encountered
                    end_time = start_time + datetime.timedelta(days=15)
                    end_time = min(end_time, end_date)  # Ensure we do not exceed the overall end date
                    end_time_str = end_time.astimezone(est).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
                    logger.info(f"Retrying with new end time: {end_time_str}")
                    time.sleep(retry_interval)  # Wait before retrying
                else:
                    logger.error(f"HTTP error occurred: {e}")
                    break

            except Exception as e:
                logger.error(f"An error occurred for {timestamp}: {e}")
                break

        if not data_fetched:
            logger.warning(f"Skipping time frame: {start_time_str} to {end_time_str}")
        
        # Trigger Airbyte sync
        try:
            airbyte_client.trigger_sync(connection_id=AIRBYTE_CONNECTION_ID)
            logger.info(f"Airbyte sync triggered successfully for {timestamp} data.")
        except Exception as e:
            logger.error(f"Failed to trigger Airbyte sync for {timestamp}: {e}")

        # Move to the next month
        current_date = end_time



if __name__ == "__main__":
    main()
