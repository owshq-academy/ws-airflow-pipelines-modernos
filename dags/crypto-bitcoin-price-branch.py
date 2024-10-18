import requests
import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

bitcoin_api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
price_threshold = 70000


@dag(
    dag_id="crypto-bitcoin-price-branch",
    schedule="@daily",
    start_date=datetime(2024, 10, 18),
    catchup=False
)
def bitcoin_branch():

    start_task = EmptyOperator(task_id="start")

    @task(task_id="fetch_bitcoin_price")
    def fetch_bitcoin_price():
        """Fetch the price of Bitcoin from the API."""

        response = requests.get(bitcoin_api_url).json()
        price = response["bitcoin"]["usd"]
        logging.info(f"The current price of Bitcoin is ${price}")
        return price

    # TODO branching logic to decide whether to process or skip based on price
    @task.branch(task_id="branch_decision")
    def branch_based_on_price(price: float):
        """Branching task to decide based on Bitcoin price."""

        if price > price_threshold:
            logging.info(f"Bitcoin price (${price}) is above the threshold.")
            return "process_price"
        else:
            logging.info(f"Bitcoin price (${price}) is below the threshold.")
            return "skip_processing"

    @task(task_id="process_price")
    def process_price():
        logging.info("Processing Bitcoin price as it's above the threshold.")
        return "Price processed"

    # TODO task for skipping the processing (if below the threshold)
    skip_processing = EmptyOperator(task_id="skip_processing")

    # TODO join task to synchronize the branches
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success"
    )

    # TODO task dependencies
    price = fetch_bitcoin_price()  # TODO Execute task and capture its return value
    decision = branch_based_on_price(price)  # TODO Use the returned value in the branching task
    start_task >> price >> decision  # TODO Define task dependencies
    decision >> process_price() >> join  # TODO Branch to process_price
    decision >> skip_processing >> join  # TODO Branch to skip_processing


bitcoin_branch()
