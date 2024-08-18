from dagster import job, op, Failure, ScheduleDefinition, Out, Int, graph
import os
from dotenv import load_dotenv
from connectors.s3_client import S3Client
from connectors.airbyte_client import AirbyteClient
from connectors.usgs_client import USGSClient
import datetime
import logging
import pytz
import subprocess
import time
#
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
    response = client.fetch_data(start_time_str, end_time_str)
    
    if response and 'error' not in response:
        logger.debug("Successfully fetched data from USGS")
        return response
    else:
        logger.error("Failed to fetch data from USGS")
        raise Failure(f"USGS API Error: {response.get('error', 'Unknown error')}")

@op
def upload_to_s3(data: dict) -> None:
    s3_client = S3Client(os.getenv('S3_BUCKET'), os.getenv('AWS_REGION'))
    filename = f"{start_time_est_str}.json"  # Use EST formatted date-time for the file name
    s3_key = f"{os.getenv('CURRENT_PREFIX')}/{filename}"
    logger.debug(f"Uploading {filename} to S3 at {s3_key}")
    s3_client.upload_to_s3(data, s3_key)

@op
def trigger_sync(context) -> str:
    airbyte_client = AirbyteClient(
        server_name=os.getenv('AIRBYTE_SERVER_NAME'),
        username=os.getenv('AIRBYTE_USERNAME'),
        password=os.getenv('AIRBYTE_PASSWORD')
    )
    
    connection_id = os.getenv('AIRBYTE_CONNECTION_ID')

    try:
        if airbyte_client.is_sync_running(connection_id):
            raise Failure("A sync job is already running. Cannot trigger another sync.")

        job_id = airbyte_client.trigger_sync(connection_id)
        context.log.info(f"Airbyte sync triggered successfully. Job ID: {job_id}")
        return job_id

    except Exception as e:
        context.log.error(f"Error in triggering Airbyte sync: {str(e)}")
        raise Failure(f"Error in triggering Airbyte sync: {str(e)}")

@op
def wait_for_sync_completion(context, job_id: str, timeout: int = 300) -> bool:
    """Wait for the Airbyte sync job to complete."""
    start_time = time.time()
    context.log.debug(f"Starting to wait for sync job with ID: {job_id}")
    
    airbyte_client = AirbyteClient(
        server_name=os.getenv('AIRBYTE_SERVER_NAME'),
        username=os.getenv('AIRBYTE_USERNAME'),
        password=os.getenv('AIRBYTE_PASSWORD')
    )

    while True:
        try:
            job_status = airbyte_client.get_sync_job_status(job_id)
            context.log.debug(f"Checked job status: {job_status}")

            if job_status['status'] == 'succeeded':
                context.log.info(f"Sync job {job_id} succeeded.")
                return True
            elif job_status['status'] == 'failed':
                context.log.error(f"Sync job {job_id} failed with error: {job_status.get('error_message', 'No error message available')}")
                return False
            elif job_status['status'] == 'running':
                context.log.debug(f"Sync job {job_id} is still running. Waiting...")
                time.sleep(10)  # Wait before checking status again
            else:
                context.log.warning(f"Unknown status for sync job {job_id}: {job_status['status']}")
                time.sleep(10)  # Wait before checking status again

        except Exception as e:
            context.log.error(f"Exception occurred while waiting for sync job {job_id}: {str(e)}")
            return False

        if time.time() - start_time > timeout:
            context.log.error(f"Timeout exceeded while waiting for sync job {job_id}.")
            return False

@op
def run_dbt() -> None:
    base_dir = os.path.join(os.path.dirname(__file__), '../../dbt_earthquake')
    profiles_dir = os.path.abspath(base_dir)
    dbt_project_dir = os.path.abspath(base_dir)
    
    logger.debug(f"Profiles directory: {profiles_dir}")
    logger.debug(f"DBT project directory: {dbt_project_dir}")
    
    if not os.path.exists(profiles_dir):
        raise Exception(f"Profiles directory does not exist: {profiles_dir}")
    if not os.path.exists(dbt_project_dir):
        raise Exception(f"DBT project directory does not exist: {dbt_project_dir}")
    
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", profiles_dir, "--project-dir", dbt_project_dir],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    else:
        logger.info(f"dbt run succeeded: {result.stdout}")

@job
def earthquake_pipeline():
    data = fetch_earthquake_data()
    upload_to_s3(data)
    job_id = trigger_sync()
    wait_for_sync_completion(job_id)
    run_dbt()  # This will only run after wait_for_sync_completion is successful

# Define a schedule for the pipeline
schedule = ScheduleDefinition(
    job=earthquake_pipeline,
    cron_schedule="* * * * *",  # Runs every minute (every 60 seconds approximated with 1 minute)
    execution_timezone="US/Eastern"  # Adjust according to your timezone
)