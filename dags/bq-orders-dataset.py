from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

GCP_PROJECT_ID = "silver-charmer-243611"
BIGQUERY_CONN_ID = "google_cloud_default"

orders_dataset = Dataset("bigquery://OwsHQ.orders")

default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bq-orders-dataset",
    start_date=datetime(2024, 9, 3),
    max_active_runs=1,
    schedule=[orders_dataset],
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'transform', 'bigquery', 'bq']
)
def process_orders():

    init = EmptyOperator(task_id="init")

    calculate_daily_totals = BigQueryInsertJobOperator(
        task_id="calculate_daily_totals",
        configuration={
            "query": {
                "query": """
                SELECT 
                    DATE(order_date) as order_day,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_amount
                FROM 
                    `OwsHQ.orders`
                GROUP BY 
                    DATE(order_date)
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": "OwsHQ",
                    "tableId": "daily_order_totals"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
        gcp_conn_id=BIGQUERY_CONN_ID
    )

    finish = EmptyOperator(task_id="finish")

    init >> calculate_daily_totals >> finish


dag = process_orders()
