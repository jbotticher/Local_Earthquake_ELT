from dagster import job, op
import os
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient
import datetime
import logging

#Configure Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure environmental variables
load_dotenv()

@op
def fetch_earthquake_data() -> dict:
    start_time_str = "2024-08-06T00:00:00Z"
    end_time_str = "2024-08-07T00:00:00Z"
    client = USGSClient(os.getenv('USGS_URL'))
    return client.fetch_data(start_time_str, end_time_str)
 

@op
def move_old_files() -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    s3_client.move_old_files(os.getenv('CURRENT_PREFIX'), os.getenv('HISTORICAL_PREFIX'))


# # @op
# # def upload_to_s3(data: dict) -> None:
# #     s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
# #     filename = f"{data['start_time']}.json"
# #     s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
# #     s3_client.upload_to_s3(data, s3_key)

@op
def upload_to_s3(data: dict) -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    logger.debug(f"Data received for S3 upload: {data}")
    try:
        filename = f"{data['start_time']}.json"
    except KeyError as e:
        logger.error(f"Missing expected key in data: {e}")
        raise
    s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
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
