import boto3
import json
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self, bucket_name: str, region_name: str = 'us-east-2'):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name

    def upload_to_s3(self, data, s3_key):
        try:
            # Remove the folder path from s3_key to upload directly to the root of the bucket
            key_without_folder = s3_key.split('/')[-1]
            
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key_without_folder, Body=json.dumps(data))
            logger.info(f"Data successfully uploaded to s3://{self.bucket_name}/{key_without_folder}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Credentials error while accessing S3: {e}")
            raise
        except ClientError as e:
            logger.error(f"Client error while uploading to S3: {e}")
            raise