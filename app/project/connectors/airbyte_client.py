import time
import requests
import base64
import logging


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
                raise Exception(f"Airbyte connection is not valid. Status code: {response.status_code}. Error message: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Exception occurred during health check: {str(e)}")
            raise Exception(f"Exception occurred during health check: {str(e)}")

    def check_job_status(self, job_id: str):
        """Check the status of a job"""
        job_status_url = f"http://{self.server_name}:8001/api/public/v1/jobs/{job_id}"
        logging.debug(f"Requesting job status from {job_status_url}")
        job_response = requests.get(url=job_status_url, headers=self.headers)
        logging.debug(f"Job status response: Status code: {job_response.status_code} - Response text: {job_response.text}")
        
        if job_response.status_code == 200:
            job_data = job_response.json()
            return job_data.get("status")
        else:
            logging.error(f"Failed to get job status. Status code: {job_response.status_code}. Error message: {job_response.text}")
            raise Exception(f"Failed to get job status. Status code: {job_response.status_code}. Error message: {response.text}")

    def trigger_sync(self, connection_id: str):
        """Trigger sync for a connection_id"""
        url = f"http://{self.server_name}:8001/api/public/v1/jobs"
        
        try:
            data = {"connectionId": connection_id, "jobType": "sync"}
            logging.info(f"Triggering Airbyte sync job with connection ID: {connection_id} at {url}")
            
            response = requests.post(url=url, json=data, headers=self.headers)
            logging.debug(f"Sync job trigger response: Status code: {response.status_code} - Response text: {response.text}")
            
            if response.status_code != 200:
                logging.error(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")
                raise Exception(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")

            job_id = response.json().get("jobId")
            if not job_id:
                logging.error(f"No jobId returned in response. Response: {response.text}")
                raise Exception(f"No jobId returned in response. Response: {response.text}")

            logging.info(f"Sync job triggered successfully. Job ID: {job_id}")

            return job_id  # Return job_id to be used in checking job status

        except requests.RequestException as e:
            logging.error(f"Exception occurred while triggering sync job: {str(e)}")
            raise Exception(f"Exception occurred while triggering sync job: {str(e)}")
