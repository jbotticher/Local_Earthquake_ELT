import time
import requests
import base64
from dagster import asset
from dagster_elt.resources import AirbyteResource




@asset
def raw_earthquake(context, airbyte_conn: AirbyteResource) -> None:
    # Authentication setup
    token = base64.b64encode(f"{airbyte_conn.username}:{airbyte_conn.password}".encode()).decode()
    headers = {"Authorization": f"Basic {token}"}

    def valid_connection() -> bool:
        """Check if connection is valid"""
        url = f"http://{airbyte_conn.server_name}:8001/api/public/v1/health"
        context.log.info(f"Checking Airbyte server health at {url}")
        try:
            response = requests.get(url=url, headers=headers)
            if response.status_code == 200:
                context.log.info("Airbyte connection is valid.")
                return True
            else:
                context.log.error(f"Airbyte connection is not valid. Status code: {response.status_code}. Error message: {response.text}")
                raise Exception(f"Airbyte connection is not valid. Status code: {response.status_code}. Error message: {response.text}")
        except requests.RequestException as e:
            context.log.error(f"Exception occurred during health check: {str(e)}")
            raise Exception(f"Exception occurred during health check: {str(e)}")

    def check_job_status(job_id: str):
        """Check the status of a job"""
        job_status_url = f"http://{airbyte_conn.server_name}:8001/api/public/v1/jobs/{job_id}"
        context.log.debug(f"Requesting job status from {job_status_url}")
        job_response = requests.get(url=job_status_url, headers=headers)
        context.log.debug(f"Job status response: Status code: {job_response.status_code} - Response text: {job_response.text}")
        
        if job_response.status_code == 200:
            job_data = job_response.json()
            return job_data.get("status")
        else:
            context.log.error(f"Failed to get job status. Status code: {job_response.status_code}. Error message: {job_response.text}")
            raise Exception(f"Failed to get job status. Status code: {job_response.status_code}. Error message: {job_response.text}")

    def trigger_sync(connection_id: str):
        """Trigger sync for a connection_id"""
        url = f"http://{airbyte_conn.server_name}:8001/api/public/v1/jobs"
        
        try:
            data = {"connectionId": connection_id, "jobType": "sync"}
            context.log.info(f"Triggering Airbyte sync job with connection ID: {connection_id} at {url}")
            
            response = requests.post(url=url, json=data, headers=headers)
            context.log.debug(f"Sync job trigger response: Status code: {response.status_code} - Response text: {response.text}")
            
            if response.status_code != 200:
                context.log.error(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")
                raise Exception(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")

            job_id = response.json().get("jobId")
            if not job_id:
                context.log.error(f"No jobId returned in response. Response: {response.text}")
                raise Exception(f"No jobId returned in response. Response: {response.text}")

            context.log.info(f"Sync job triggered successfully. Job ID: {job_id}")

            return job_id  # Return job_id to be used in checking job status

        except requests.RequestException as e:
            context.log.error(f"Exception occurred while triggering sync job: {str(e)}")
            raise Exception(f"Exception occurred while triggering sync job: {str(e)}")

    # Execute the Airbyte workflow
    context.log.info(f"Triggering Airbyte sync for connection ID: {airbyte_conn.connection_id}")

    try:
        # Check connection validity
        if not valid_connection():
            raise Exception("Failed to establish a valid connection to Airbyte")

        # Trigger the sync job
        job_id = trigger_sync(airbyte_conn.connection_id)

        # Check the status of the sync job
        context.log.info(f"Checking status of Airbyte sync job with Job ID: {job_id}")
        while True:
            status = check_job_status(job_id)
            if status == "succeeded":
                context.log.info(f"Airbyte sync job {job_id} completed successfully.")
                return  # Successfully completed
            elif status == "failed":
                context.log.error(f"Airbyte sync job {job_id} failed.")
                raise Exception(f"Airbyte sync job {job_id} failed.")
            else:
                context.log.info(f"Job {job_id} is still in progress. Checking again in 10 seconds.")
                time.sleep(10)  # Poll every 10 seconds

    except Exception as e:
        context.log.error(f"Error triggering or checking Airbyte sync: {e}")
        raise
