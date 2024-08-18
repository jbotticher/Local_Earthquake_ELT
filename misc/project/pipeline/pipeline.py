# import os
# import datetime
# import logging
# import pytz
# import schedule
# import time
# from dotenv import load_dotenv
# from connectors.s3_client import S3Client
# from connectors.airbyte_client import AirbyteClient
# from connectors.usgs_client import USGSClient

# # Configure Logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# # Configure environmental variables
# load_dotenv()

# # Calculate start_time and end_time
# def calculate_times():
#     start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)  # Current date - 1 day
#     end_time = datetime.datetime.utcnow()

#     # Convert UTC time to EST
#     est = pytz.timezone('US/Eastern')
#     start_time_est = start_time.astimezone(est)
#     end_time_est = end_time.astimezone(est)

#     # Format dates in ISO 8601 format for USGS API
#     start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
#     end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

#     # Format dates in EST for the S3 file name
#     start_time_est_str = start_time_est.strftime('%Y-%m-%dT%H-%M-%S')  # Example: 2023-08-09T07-30-00

#     return start_time_str, end_time_str, start_time_est_str

# def fetch_earthquake_data():
#     start_time_str, end_time_str, _ = calculate_times()
#     client = USGSClient(os.getenv('USGS_URL'))
#     return client.fetch_data(start_time_str, end_time_str)

# def upload_to_s3(data):
#     _, _, start_time_est_str = calculate_times()
#     s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
#     filename = f"{start_time_est_str}.json"  # Use EST formatted date-time for the file name
#     s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
#     logger.debug(f"Filename: {filename}")
#     logger.debug(f"S3 Key: {s3_key}")
#     s3_client.upload_to_s3(data, s3_key)

# def trigger_sync():
#     airbyte_client = AirbyteClient(
#         server_name=os.getenv('AIRBYTE_SERVER_NAME'),
#         username=os.getenv('AIRBYTE_USERNAME'),
#         password=os.getenv('AIRBYTE_PASSWORD')
#     )
#     airbyte_client.trigger_sync(os.getenv('AIRBYTE_CONNECTION_ID'))

# def job():
#     try:
#         data = fetch_earthquake_data()
#         upload_to_s3(data)
#         trigger_sync()
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")

# # Schedule the job to run every 60 seconds
# schedule.every(60).seconds.do(job)

# # Main loop to keep the script running
# if __name__ == "__main__":
#     logger.info("Starting the scheduler...")
#     while True:
#         schedule.run_pending()
#         time.sleep(1)



import os
import datetime
import logging
import pytz
import time
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient

# Configure Logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configure environmental variables
load_dotenv()

# Calculate start_time and end_time
def calculate_times():
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

    logger.debug(f"Calculated times - Start Time (UTC): {start_time_str}, End Time (UTC): {end_time_str}, Start Time (EST): {start_time_est_str}")
    return start_time_str, end_time_str, start_time_est_str

def fetch_earthquake_data():
    start_time_str, end_time_str, _ = calculate_times()
    logger.info(f"Fetching earthquake data from USGS API for period: {start_time_str} to {end_time_str}")
    client = USGSClient(os.getenv('USGS_URL'))
    data = client.fetch_data(start_time_str, end_time_str)
    logger.info(f"Fetched {len(data)} records from USGS API")
    return data

def upload_to_s3(data):
    _, _, start_time_est_str = calculate_times()
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    filename = f"{start_time_est_str}.json"  # Use EST formatted date-time for the file name
    s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
    logger.info(f"Uploading data to S3 - Filename: {filename}, S3 Key: {s3_key}")
    s3_client.upload_to_s3(data, s3_key)
    logger.info("Data uploaded successfully")

def trigger_sync():
    airbyte_client = AirbyteClient(
        server_name=os.getenv('AIRBYTE_SERVER_NAME'),
        username=os.getenv('AIRBYTE_USERNAME'),
        password=os.getenv('AIRBYTE_PASSWORD')
    )
    logger.info(f"Triggering Airbyte sync for connection ID: {os.getenv('AIRBYTE_CONNECTION_ID')}")
    job_id = airbyte_client.trigger_sync(os.getenv('AIRBYTE_CONNECTION_ID'))
    logger.info(f"Triggered Airbyte sync job with Job ID: {job_id}")
    
    # Check the status of the sync job
    logger.info(f"Checking status of Airbyte sync job with Job ID: {job_id}")
    job_status = airbyte_client.check_job_status(job_id)
    logger.info(f"Airbyte sync job completed with status: {job_status}")

def job():
    logger.info("Starting job execution...")
    try:
        data = fetch_earthquake_data()
        upload_to_s3(data)
        trigger_sync()
        logger.info("Job executed successfully")
    except Exception as e:
        logger.error(f"An error occurred during job execution: {e}")

# Main loop to run the job immediately and then every 5 seconds
if __name__ == "__main__":
    logger.info("Starting the job scheduler...")
    while True:
        job()  # Run the job immediately
        logger.info("Waiting for 5 seconds before the next run...")
        time.sleep(5)  # Wait for 5 seconds before running the job again


# import datetime
# import json
# import logging
# import time
# import os
# from dotenv import load_dotenv
# from connectors.s3_client import S3Client
# from connectors.airbyte_client import AirbyteClient
# from connectors.usgs_client import USGSClient



# def fetch_earthquake_data(start_time_str, end_time_str):
#     client = USGSClient(USGS_URL)
#     return client.fetch_data(start_time_str, end_time_str)

# def main():
#     s3_client = S3Client(S3_BUCKET, AWS_REGION)
#     airbyte_client = AirbyteClient(
#         server_name=AIRBYTE_SERVER_NAME, 
#         username=AIRBYTE_USERNAME, 
#         password=AIRBYTE_PASSWORD
#     )
    
#     while True:
#         try:
#             # Validate Airbyte connection
#             if not airbyte_client.valid_connection():
#                 raise Exception("Invalid Airbyte connection")

#             # Calculate the date range for yesterday
#             end_time = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
#             start_time = end_time - datetime.timedelta(days=1)

#             # Format dates
#             start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
#             end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
#             filename = f"{start_time.strftime('%Y-%m-%d')}.json"
#             s3_key = f'{CURRENT_PREFIX}/{filename}'


#             # Fetch data and upload to S3
#             data = fetch_earthquake_data(start_time_str, end_time_str)
#             s3_client.upload_to_s3(data, s3_key)

#             # Trigger Airbyte sync
#             airbyte_client.trigger_sync(connection_id=AIRBYTE_CONNECTION_ID)

#             # Log success message and wait before the next iteration
#             logger.info("Job successful, waiting 60 seconds until next run.")
#             time.sleep(60)
        
#         except Exception as e:
#             logger.error(f"An error occurred: {e}")
#             # Optional: sleep for a longer period before retrying to avoid rapid retries in case of persistent errors
#             time.sleep(60)

# if __name__ == "__main__":
#     # Load environment variables from .env file
#     load_dotenv()
    
#     # Configure logging
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#     logger = logging.getLogger(__name__)
    
#     # Configuration
#     USGS_URL = os.environ.get('USGS_URL')
#     S3_BUCKET = os.environ.get('S3_BUCKET')
#     CURRENT_PREFIX = os.environ.get('CURRENT_PREFIX')
#     HISTORICAL_PREFIX = os.environ.get('HISTORICAL_PREFIX')
#     AWS_REGION = os.environ.get('AWS_REGION')
    
#     AIRBYTE_SERVER_NAME = os.environ.get('AIRBYTE_SERVER_NAME')
#     AIRBYTE_USERNAME = os.environ.get('AIRBYTE_USERNAME')
#     AIRBYTE_PASSWORD = os.environ.get('AIRBYTE_PASSWORD')
#     AIRBYTE_CONNECTION_ID = os.environ.get('AIRBYTE_CONNECTION_ID')

#     # Run the pipeline
#     main()