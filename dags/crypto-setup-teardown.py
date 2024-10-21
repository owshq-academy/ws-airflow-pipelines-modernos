import requests
import logging
from airflow.decorators import dag, task, setup, teardown
from pendulum import datetime

# TODO constants
bitcoin_api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
price_threshold = 50000
log_file_path = "/tmp/crypto_log.txt"


@dag(
    dag_id="crypto-setup-teardown",
    schedule="@daily",
    start_date=datetime(2024, 10, 18),
    catchup=False
)
def crypto_price_workflow():

    # TODO setup task to initialize the environment (e.g., logging setup)
    @setup
    def setup_environment():
        """Setup task to prepare the environment before processing."""

        logging.info("Setting up the environment for cryptocurrency price workflow.")
        with open(log_file_path, "a") as log_file:
            log_file.write("Starting cryptocurrency price workflow.\n")

    @task
    def fetch_bitcoin_price():
        """Fetch the price of Bitcoin from the API."""

        response = requests.get(bitcoin_api_url).json()
        price = response["bitcoin"]["usd"]
        logging.info(f"The current price of Bitcoin is ${price}")
        return price

    @task.branch
    def branch_based_on_price(price: float):
        """Branch based on Bitcoin price."""
        if price > price_threshold:
            logging.info(f"Bitcoin price (${price}) is above the threshold.")
            return "process_high_price"
        else:
            logging.info(f"Bitcoin price (${price}) is below the threshold.")
            return "process_low_price"

    @task
    def process_high_price():
        logging.info("Processing high price for Bitcoin.")
        with open(log_file_path, "a") as log_file:
            log_file.write("Bitcoin price processed as high.\n")

    @task
    def process_low_price():
        logging.info("Processing low price for Bitcoin.")
        with open(log_file_path, "a") as log_file:
            log_file.write("Bitcoin price processed as low.\n")

    # TODO teardown task to clean up after the main tasks (e.g., finalize logging)
    @teardown
    def teardown_environment():
        """Teardown task to finalize the environment after the workflow."""

        logging.info("Tearing down the environment after cryptocurrency price workflow.")
        with open(log_file_path, "a") as log_file:
            log_file.write("Cryptocurrency price workflow completed.\n")

    setup_task = setup_environment()
    teardown_task = teardown_environment()
    price = fetch_bitcoin_price()
    decision = branch_based_on_price(price)

    high_price_processing = process_high_price()
    low_price_processing = process_low_price()

    setup_task >> price >> decision
    decision >> high_price_processing >> teardown_task
    decision >> low_price_processing >> teardown_task


crypto_price_workflow()
