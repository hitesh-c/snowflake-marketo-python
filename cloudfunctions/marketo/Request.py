import requests
import os
import json
from dotenv import load_dotenv
import logging
from requests.exceptions import RequestException

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomException(Exception):
    def __init__(self, success, message, operation ):
        self.success = success
        self.message = message
        self.operation = operation
        self.table_name = "MARKETO_REQUEST"
        super().__init__(message)

class Request:
    def __init__(self):
        self.client_id = os.environ.get("MARKETO_CLIENT_ID")
        self.client_secret = os.environ.get("MARKETO_CLIENT_SECRET")
        self.base_url = os.environ.get("MARKETO_BASE_URL")
        self.access_token = self.authenticate()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def authenticate(self):
        logger.info("Authenticating to Marketo")
        try:
            auth_url = f"{self.base_url}/identity/oauth/token"
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
            response = requests.post(auth_url, data=payload)
            response.raise_for_status()
            access_token = response.json()["access_token"]
            logger.info("Successfully authenticated to Marketo")
            return access_token
        except RequestException as e:
            logger.error(f"Error in Marketo Authentication: {e}")
           
    def get(self, url, params=None, retry= 3):
        if retry == 0:
            logger.error(f"Error fetching url: {self.base_url}{url}. Params: {params}")
            raise CustomException(success=False, message=f"Error fetching url: {self.base_url}{url}. Params: {params}.", operation="get")
        try:
            if params:
                params["access_token"] = self.access_token
            response = requests.get(f"{self.base_url}{url}", headers=self.headers, params=params)
            response.raise_for_status()
            return response
        except RequestException as e:
            print(e)
            self.authenticate()
            self.get(url, params=params, retry=retry-1)

    def post(self, url, data=None, retry=3):
        if retry == 0:
            logger.error(f"Error fetching url: {self.base_url}{url}. Params: {params}. Error: {e}")
            raise CustomException(success=False, message=f"Error posting url: {self.base_url}{url}. Data: {data}. Error: {e}", operation="post")
        try:
            response = requests.post(f"{self.base_url}{url}", headers=self.headers, data=json.dumps(data))
            response.raise_for_status()
            return response
        except RequestException as e:
            self.authenticate()
            self.post(url, data=data, retry=retry-1)
