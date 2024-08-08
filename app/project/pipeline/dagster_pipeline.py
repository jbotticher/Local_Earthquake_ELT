from dagster import job, op
import os
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient
import datetime
import logging

# Configure Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure environmental variables
load_dotenv()

# Define start_time and end_time
start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)  # Example: 1 day ago
end_time = datetime.datetime.utcnow()

# Format dates
start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

@op
def fetch_earthquake_data() -> dict:
    client = USGSClient(os.getenv('USGS_URL'))
    return client.fetch_data(start_time_str, end_time_str)

@op
def move_old_files() -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    s3_client.move_old_files(os.getenv('CURRENT_PREFIX'), os.getenv('HISTORICAL_PREFIX'))

# @op
# def upload_to_s3(data: dict) -> None:
#     s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
#     filename = f"{start_time_str}.json"
#     s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
#     s3_client.upload_to_s3(data, s3_key)

@op
def upload_to_s3(data: dict) -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    filename = f"{start_time_str}.json"
    s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
    logger.debug(f"Filename: {filename}")
    logger.debug(f"S3 Key: {s3_key}")
    s3_client.upload_to_s3(data, s3_key)

@op
def trigger_sync() -> None:
    airbyte_client = AirbyteClient(
        server_name=os.getenv('AIRBYTE_SERVER_NAME'),
        username=os.getenv('AIRBYTE_USERNAME'),
        password=os.getenv('AIRBYTE_PASSWORD')
    )
    airbyte_client.trigger_sync(os.getenv('AIRBYTE_CONNECTION_ID'))

@job
def earthquake_pipeline():
    data = fetch_earthquake_data()
    move_old_files()
    upload_to_s3(data)
    # trigger_sync()
