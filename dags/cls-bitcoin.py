import logging
import requests

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


# TODO extract
def extract_bitcoin():
    return requests.get(API).json()["bitcoin"]


# TODO process
def process_bitcoin(ti):
    response = ti.xcom_pull(task_ids="extract_bitcoin_from_api")
    logging.info(response)
    processed_data = {"usd": response["usd"], "change": response["usd_24h_change"]}
    ti.xcom_push(key="processed_data", value=processed_data)


# TODO store
def store_bitcoin(ti):
    data = ti.xcom_pull(task_ids="process_bitcoin", key="processed_data")
    logging.info(data)


with DAG(
    dag_id="cls-bitcoin",
    schedule="@daily",
    start_date=datetime(2021, 12, 1),
    catchup=False
):

    # TODO Task 1
    extract_bitcoin_from_api = PythonOperator(
        task_id="extract_bitcoin_from_api",
        python_callable=extract_bitcoin
    )

    # TODO Task 2
    process_bitcoin_from_api = PythonOperator(
        task_id="process_bitcoin_from_api",
        python_callable=process_bitcoin
    )

    # TODO Task 3
    store_bitcoin_from_api = PythonOperator(
        task_id="store_bitcoin_from_api",
        python_callable=store_bitcoin
    )

    # TODO set dependencies
    extract_bitcoin_from_api >> process_bitcoin_from_api >> store_bitcoin_from_api
