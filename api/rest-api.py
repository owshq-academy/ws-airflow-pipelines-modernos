# TODO import libraries
import requests
import json

from requests.auth import HTTPBasicAuth
from datetime import datetime

# TODO set airflow configs
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/airbyte-sync-astro-sdk-bq/dagRuns"
USERNAME = "admin"
PASSWORD = "admin"

# TODO create a unique dag_run_id using the current timestamp
dag_run_id = f"manual_run_airbyte_sync_astro_sdk_bq_{datetime.now().strftime('%Y%m%d%H%M%S')}"

# TODO request payload
payload = {
    "dag_run_id": dag_run_id,
    "conf": {}
}

# TODO make the POST request to trigger the DAG
response = requests.post(
    AIRFLOW_API_URL,
    auth=HTTPBasicAuth(USERNAME, PASSWORD),
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload)
)

# TODO check the response
if response.status_code == 200:
    print(f"Successfully triggered DAG with run ID: {dag_run_id}")
else:
    print(f"Failed to trigger DAG. Status Code: {response.status_code}")
    print(f"Response: {response.text}")
