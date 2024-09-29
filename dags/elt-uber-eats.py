from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup

from airflow.datasets import Dataset

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

GCS_CONN_ID = "google_cloud_default"
BIGQUERY_CONN_ID = "google_cloud_default"

# TODO define the dataset
orders_dataset = Dataset("bigquery://OwsHQ.orders")

default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="elt-uber-eats",
    start_date=datetime(2024, 9, 3),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'elt', 'astrosdk', 'gcs', 'bq']
)
def load_json_files():

    init = EmptyOperator(task_id="init")

    # TODO GCS operator to check if the file exists
    with TaskGroup(group_id="Detection") as detection_group:
        wait_for_file = GCSObjectsWithPrefixExistenceSensor(
            task_id="wait_for_file",
            bucket="owshq-uber-eats-files",
            prefix="com.owshq.data/kafka/orders/",
            google_cloud_conn_id=GCS_CONN_ID,
            timeout=3600,
            poke_interval=30,
            deferrable=True
        )

    # TODO Astro Python SDK [operator] task to load the JSON file into BigQuery
    # TODO extend the capability by using the [native transfer] option
    with TaskGroup(group_id="Orders") as orders_group:
        load_orders_json_file = aql.load_file(
            task_id="load_orders_json_file",
            input_file=File(
                path="gs://owshq-uber-eats-files/com.owshq.data/kafka/orders/",
                filetype=FileType.JSON,
                conn_id=GCS_CONN_ID
            ),
            output_table=Table(
                name="orders",
                metadata=Metadata(schema="OwsHQ"),
                conn_id=BIGQUERY_CONN_ID
            ),
            if_exists="append",
            use_native_support=True,
            columns_names_capitalization="original",
            outlets=[orders_dataset]
        )

        load_orders_json_file

    with TaskGroup(group_id="Delete") as cleanup_group:
        delete_landing_json_files = GCSDeleteObjectsOperator(
            task_id="delete_landing_json_files",
            bucket_name="owshq-uber-eats-files",
            prefix="com.owshq.data/kafka/orders/",
            gcp_conn_id=GCS_CONN_ID
        )

    finish = EmptyOperator(task_id="finish")

    init >> detection_group >> orders_group >> cleanup_group >> finish


dag = load_json_files()
