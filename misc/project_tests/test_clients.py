import pytest
from unittest.mock import patch, MagicMock
from project.connectors.airbyte_client import AirbyteClient
from project.connectors.s3_client import S3Client
from project.connectors.usgs_client import USGSClient
import requests


# AirbyteClient tests
@pytest.fixture
def airbyte_client():
    return AirbyteClient(server_name="localhost", username="user", password="pass")

def test_valid_connection(airbyte_client):
    with patch("requests.get") as mocked_get:
        mocked_get.return_value.status_code = 200
        assert airbyte_client.valid_connection()

def test_invalid_connection(airbyte_client):
    with patch("requests.get") as mocked_get:
        mocked_get.return_value.status_code = 500
        with pytest.raises(Exception):
            airbyte_client.valid_connection()

def test_trigger_sync(airbyte_client):
    with patch("requests.post") as mocked_post, patch("requests.get") as mocked_get:
        mocked_post.return_value.status_code = 200
        mocked_post.return_value.json.return_value = {"jobId": "123"}
        mocked_get.return_value.status_code = 200
        mocked_get.return_value.json.return_value = {"status": "succeeded"}
        airbyte_client.trigger_sync(connection_id="test-id")


# S3Client tests
@pytest.fixture
def s3_client():
    return S3Client(bucket_name="test-bucket")

def test_upload_to_s3(s3_client):
    with patch.object(s3_client.s3_client, 'put_object') as mocked_put:
        s3_client.upload_to_s3(data={"key": "value"}, s3_key="test/key")
        mocked_put.assert_called_once()

def test_move_old_files(s3_client):
    with patch.object(s3_client.s3_client, 'list_objects_v2') as mocked_list, \
         patch.object(s3_client.s3_client, 'copy_object') as mocked_copy, \
         patch.object(s3_client.s3_client, 'delete_object') as mocked_delete:
        mocked_list.return_value = {'Contents': [{'Key': 'test/key/file.json'}]}
        s3_client.move_old_files(current_prefix="test/key/", destination_bucket="test-dest-bucket")
        mocked_copy.assert_called_once()
        mocked_delete.assert_called_once()


# USGSClient tests
@pytest.fixture
def usgs_client():
    return USGSClient(url="https://earthquake.usgs.gov/fdsnws/event/1/query")

def test_fetch_data(usgs_client):
    with patch("requests.get") as mocked_get:
        mocked_get.return_value.status_code = 200
        mocked_get.return_value.json.return_value = {"type": "FeatureCollection"}
        result = usgs_client.fetch_data(start_time_str="2024-08-01", end_time_str="2024-08-02")
        assert result["type"] == "FeatureCollection"

def test_fetch_data_failure(usgs_client):
    with patch("requests.get") as mocked_get:
        # Simulate a response with status code 500
        mocked_get.return_value.status_code = 500
        mocked_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        # Assert that HTTPError is raised
        with pytest.raises(requests.exceptions.HTTPError):
            usgs_client.fetch_data(start_time_str="2024-08-01", end_time_str="2024-08-02")
