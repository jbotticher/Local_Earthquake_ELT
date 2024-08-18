class AirbyteClient:
    def __init__(self, server_name: str, username: str, password: str):
        self.server_name = server_name
        self.username = username
        self.password = password
        self.headers = {
            "Authorization": f"Basic {base64.b64encode(f'{username}:{password}'.encode()).decode()}",
            "Content-Type": "application/json"
        }

    def is_sync_running(self, connection_id):
        try:
            response = self._get_sync_status(connection_id)
            response_data = response.json()

            # Check if response_data is a dictionary
            if isinstance(response_data, dict):
                jobs = response_data.get("jobs", [])
            elif isinstance(response_data, list):
                # Handle the case where response_data is a list
                jobs = response_data
            else:
                jobs = []

            # Further processing here
            return any(job["status"] == "running" for job in jobs if isinstance(job, dict))
        except Exception as e:
            raise Exception(f"Error in checking sync status: {str(e)}")
        
    def valid_connection(self) -> bool:
        """Check if connection is valid"""
        url = f"http://{self.server_name}:8001/api/v1/health"
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

    def trigger_sync(self, connection_id: str) -> str:
        """Trigger sync for a connection_id"""
        url = f"http://{self.server_name}:8001/api/v1/connections/sync"
        try:
            data = {"connectionId": connection_id}
            logging.info(f"Triggering Airbyte sync job with connection ID: {connection_id} at {url}")

            response = requests.post(url=url, json=data, headers=self.headers)
            if response.status_code != 200:
                logging.error(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")
                raise Exception(f"Failed to trigger sync. Status code: {response.status_code}. Error message: {response.text}")

            job_id = response.json().get("job", {}).get("id")
            if not job_id:
                logging.error(f"No jobId returned in response. Response: {response.text}")
                raise Exception(f"No jobId returned in response. Response: {response.text}")

            logging.info(f"Sync job triggered successfully. Job ID: {job_id}")
            return job_id
        except requests.RequestException as e:
            logging.error(f"Exception occurred while triggering sync job: {str(e)}")
            raise Exception(f"Exception occurred while triggering sync job: {str(e)}")

    def check_job_status(self, job_id: str) -> str:
        """Check the status of the sync job until it completes."""
        url = f"http://{self.server_name}:8001/api/v1/jobs/{job_id}"
        try:
            while True:
                response = requests.get(url=url, headers=self.headers)
                if response.status_code == 200:
                    job_status = response.json().get("job", {}).get("status")
                    if job_status in ["succeeded", "failed"]:
                        logging.info(f"Sync job {job_id} finished with status: {job_status}")
                        return job_status
                    logging.info(f"Job {job_id} is still in progress. Waiting 10 seconds before rechecking...")
                    time.sleep(10)  # Poll every 10 seconds
                else:
                    logging.error(f"Failed to get job status. Status code: {response.status_code}. Error message: {response.text}")
                    raise Exception(f"Failed to get job status. Status code: {response.status_code}. Error message: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Error checking job status: {str(e)}")
            raise Exception(f"Error checking job status: {str(e)}")



    def get_running_sync_job_id(self, connection_id: str) -> int:
        url = f"http://{self.server_name}:8001/api/v1/jobs/list"
        data = {
            "configId": connection_id,
            "configTypes": ["sync"]
        }
        response = requests.post(url, headers=self.headers, json=data)
        if response.status_code == 200:
            jobs = response.json().get("jobs", [])
            for job in jobs:
                if job["status"] == "running":
                    return job["job"]["id"]
        raise Exception("No running sync job found.")