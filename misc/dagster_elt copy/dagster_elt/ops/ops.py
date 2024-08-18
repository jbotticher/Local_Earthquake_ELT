import os
import datetime
import time
import pytz
from dotenv import load_dotenv
from dagster import op, Config, OpExecutionContext
import requests
import boto3
import json
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


class EarthquakeConfig(Config):
    usgs_url:str = 'https://earthquake.usgs.gov/fdsnws/event/1/query'


@op
def fetch_earthquake_data(context: OpExecutionContext, config: EarthquakeConfig) -> dict:
    # Calculate start_time and end_time
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)  # Current date - 1 day
    end_time = datetime.datetime.utcnow()

    # Format dates in ISO 8601 format for USGS API
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    context.log.info(f"Fetching earthquake data from USGS API for period: {start_time_str} to {end_time_str}")
    try:
        params = {
            'format': 'geojson',
            'starttime': start_time_str,
            'endtime': end_time_str,
        }
        response = requests.get(config.usgs_url, params=params)
        response.raise_for_status()  # This will raise an HTTPError if the response was not successful

        if response.status_code == 200:
            context.log.info("API retrieved data successfully")

        return response.json()
    except requests.RequestException as e:
        context.log.error(f"Failed to fetch data from USGS API: {e}")
        raise

@op
def upload_to_s3(context: OpExecutionContext, data: dict) -> None:
    load_dotenv()
    # Calculate start_time and end_time
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)  # Current date - 1 day

    # Convert UTC time to EST
    est = pytz.timezone('US/Eastern')
    start_time_est = start_time.astimezone(est)

    filename = start_time_est.strftime('%Y-%m-%dT%H-%M-%S.json')
    bucket_name = os.getenv('S3_BUCKET')
    region_name = os.getenv('AWS_REGION')

    # Initialize the S3 client
    s3_client = boto3.client('s3', region_name=region_name)
    
    # Upload data to S3
    try:
        s3_key = filename  # Directly use the filename as the S3 key
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(data))
        context.log.info(f"Data successfully uploaded to s3://{bucket_name}/{s3_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        context.log.error(f"Credentials error while accessing S3: {e}")
        raise
    except ClientError as e:
        context.log.error(f"Client error while uploading to S3: {e}")
        raise