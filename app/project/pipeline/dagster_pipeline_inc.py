from dagster import job, op, Failure
import os
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient
import datetime
import logging
import pytz



# Configure Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure environmental variables
load_dotenv()


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



@op
def fetch_earthquake_data() -> dict:
    client = USGSClient(os.getenv('USGS_URL'))
    return client.fetch_data(start_time_str, end_time_str)

@op
def move_old_files() -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    # Move files to dec-earthquake-bucket directly without any folder
    s3_client.move_old_files(os.getenv('CURRENT_PREFIX'), 'dec-earthquake-bucket')

@op
def upload_to_s3(data: dict) -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    filename = f"{start_time_est_str}.json"  # Use EST formatted date-time for the file name
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
    trigger_sync()