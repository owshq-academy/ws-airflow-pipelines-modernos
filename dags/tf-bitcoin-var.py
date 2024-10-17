import logging
import requests

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="tf-bitcoin-var",
    schedule="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=False
)
def bitcoin():

    @task(task_id="extract", retries=2)
    def extract_bitcoin():
        # TODO retrieve from VARIABLES
        api_url = Variable.get("bitcoin_api_url")
        return requests.get(api_url).json()["bitcoin"]

    @task(task_id="transform")
    def process_bitcoin(response):
        return {"usd": response["usd"], "change": response["usd_24h_change"]}

    @task(task_id="store")
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}")

    store_bitcoin(process_bitcoin(extract_bitcoin()))


bitcoin()
