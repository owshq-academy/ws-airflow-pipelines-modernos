import requests
import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime

# TODO constants
bitcoin_api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
high_price_threshold = 60000
medium_price_threshold = 40000


@dag(
    dag_id="crypto-bitcoin-price-trigger",
    schedule="@daily",
    start_date=datetime(2024, 10, 18),
    catchup=False,
    max_active_runs=1
)
def bitcoin_trigger_rule():
    start = EmptyOperator(task_id="start")

    @task(task_id="fetch_bitcoin_price")
    def fetch_bitcoin_price():
        """Fetch the price of Bitcoin from the API."""

        response = requests.get(bitcoin_api_url).json()
        price = response["bitcoin"]["usd"]
        logging.info(f"The current price of Bitcoin is ${price}")
        return price

    # TODO more complex branching logic
    @task.branch(task_id="branch_decision")
    def branch_based_on_price(price: float):
        """Branch based on Bitcoin price into different ranges."""

        if price > high_price_threshold:
            logging.info(f"Bitcoin price (${price}) is in the high range.")
            return "high_price_processing"
        elif price > medium_price_threshold:
            logging.info(f"Bitcoin price (${price}) is in the medium range.")
            return "medium_price_processing"
        else:
            logging.info(f"Bitcoin price (${price}) is in the low range.")
            return "low_price_processing"

    # TODO Task = high_price_processing
    @task(task_id="high_price_processing")
    def process_high_price():
        logging.info("Processing high price: Taking action for high Bitcoin prices.")
        return "High price processed"

    # TODO Task = medium_price_processing
    @task(task_id="medium_price_processing")
    def process_medium_price():
        logging.info("Processing medium price: Taking action for medium Bitcoin prices.")
        return "Medium price processed"

    # TODO Task = low_price_processing
    @task(task_id="low_price_processing")
    def process_low_price():
        logging.info("Processing low price: Taking action for low Bitcoin prices.")
        return "Low price processed"

    skip_processing = EmptyOperator(task_id="skip_processing")

    # TODO Trigger Rules [TR]
    # TODO NONE_FAILED_MIN_ONE_SUCCESS = task is triggered if at least one upstream task succeeds and none of them fail.
    join_task = EmptyOperator(
        task_id="join_task",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # TODO ALL_DONE = final task runs regardless of the success or failure of the upstream tasks
    final_task = EmptyOperator(
        task_id="final_task",
        trigger_rule=TriggerRule.ALL_DONE
    )

    price = fetch_bitcoin_price()
    decision = branch_based_on_price(price)

    start >> price >> decision
    decision >> process_high_price() >> join_task
    decision >> process_medium_price() >> join_task
    decision >> process_low_price() >> join_task
    decision >> skip_processing >> join_task

    join_task >> final_task


bitcoin_trigger_rule()
