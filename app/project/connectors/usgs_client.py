import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class USGSClient:
    def __init__(self, url):
        self.url = url

    def fetch_data(self, start_time_str, end_time_str):
        try:
            params = {
                'format': 'geojson',
                'starttime': start_time_str,
                'endtime': end_time_str,
            }
            response = requests.get(self.url, params=params, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from USGS API: {e}")
            raise
