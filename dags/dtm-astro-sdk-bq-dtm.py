"""
"""

# TODO Imports
from datetime import datetime, timedelta
from airflow.decorators import dag
from astro.sql.table import Table, Metadata
from astro.files import get_file_list
from astro.sql.operators.load_file import LoadFileOperator as LoadFile
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteDatasetOperator
from airflow.operators.empty import EmptyOperator

# TODO Connections & Variables
airbyte_conn_id = "airbyte_default"
airbyte_sync_atlas_gcs_id = "b546017a-336c-4610-b27f-ca5dc3d47d25"
landing_zone_path = "gs://owshq-airbyte-ingestion/"
source_gcs_conn_id = "google_cloud_default"
bq_conn_id = "google_cloud_default"

# TODO Default Arguments
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TODO DAG Definition
@dag(
    dag_id="dtm-astro-sdk-bq-dtm",
    start_date=datetime(2024, 9, 26),
    max_active_runs=1,
    schedule_interval=timedelta(hours=8),
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'ingestion', 'airbyte', 'postgres', 'mongodb', 'gcs']
)
def init():

    # TODO Tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    delete_users_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_users_dataset",
        dataset_id="OneWaySolution.users",
        delete_contents=True,
        gcp_conn_id=bq_conn_id
    )

    delete_payments_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_payments_dataset",
        dataset_id="OneWaySolution.payments",
        delete_contents=True,
        gcp_conn_id=bq_conn_id
    )

    users_file_parquet = LoadFile.partial(
        task_id="users_file_parquet",
        output_table=Table(name="users", metadata=Metadata(schema="OneWaySolution"), conn_id=bq_conn_id),
        use_native_support=True,
        if_exists="append",
        pool="dynamic"
    ).expand(input_file=get_file_list(
        path=landing_zone_path + "mongodb-atlas/users/",
        conn_id=source_gcs_conn_id)
    )

    payments_file_parquet = LoadFile.partial(
        task_id="payments_file_parquet",
        output_table=Table(name="payments", metadata=Metadata(schema="OneWaySolution"), conn_id=bq_conn_id),
        use_native_support=True,
        if_exists="append",
        pool="dynamic",
    ).expand(input_file=get_file_list(
        path=landing_zone_path + "mongodb-atlas/payments/",
        conn_id=source_gcs_conn_id)
    )

    # TODO Task Dependencies
    start >> delete_users_dataset >> users_file_parquet >> end
    start >> delete_payments_dataset >> payments_file_parquet >> end


# TODO DAG Instantiation
dag = init()
