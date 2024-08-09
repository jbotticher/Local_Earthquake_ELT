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
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=json.dumps(data))
            logger.info(f"Data successfully uploaded to s3://{self.bucket_name}/{s3_key}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Credentials error while accessing S3: {e}")
            raise
        except ClientError as e:
            logger.error(f"Client error while uploading to S3: {e}")
            raise

    def move_old_files(self, current_prefix: str, destination_bucket: str):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=current_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    
                    # Ensure that we only move files that are directly in the current_prefix folder
                    if key.startswith(current_prefix) and key != current_prefix:
                        filename = key.split('/')[-1]  # Extract the filename
                        
                        # Define new key directly in the destination bucket without any folder structure
                        new_key = filename
                        
                        # Copy the object to the destination bucket
                        self.s3_client.copy_object(
                            Bucket=destination_bucket, 
                            CopySource={'Bucket': self.bucket_name, 'Key': key}, 
                            Key=new_key
                        )
                        
                        # Delete the original object to complete the move operation
                        self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
                        logger.info(f"Moved {key} to s3://{destination_bucket}/{new_key}")
            else:
                logger.info(f"No files found in {current_prefix}")
        except ClientError as e:
            logger.error(f"Client error while moving files in S3: {e}")
            raise

    def create_folder(self, folder_key: str):
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=folder_key)
            logger.info(f"Folder {folder_key} created in S3")
        except ClientError as e:
            logger.error(f"Client error while creating folder in S3: {e}")
            raise

    def delete_object(self, key: str):
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted {key} from S3")
        except ClientError as e:
            logger.error(f"Client error while deleting object in S3: {e}")
            raise