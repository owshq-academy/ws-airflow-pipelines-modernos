"""
"""

# TODO Imports
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# TODO Connections & Variables
airbyte_conn_id = "airbyte_default"
airbyte_sync_atlas_gcs_id = "c6705b6a-a046-42e8-bcc7-6f107bca0738"
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
    dag_id="airbyte-sync-dev",
    start_date=datetime(2024, 9, 26),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'ingestion', 'airbyte', 'postgres', 'mongodb', 'gcs']
)
def init():

    # TODO Tasks Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    trigger_airbyte_sync_atlas_gcs = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        connection_id=airbyte_sync_atlas_gcs_id,
        airbyte_conn_id=airbyte_conn_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    user_parquet = aql.load_file(
        task_id="user_parquet",
        input_file=File(path=landing_zone_path + "mongodb-atlas/user/", filetype=FileType.PARQUET, conn_id=source_gcs_conn_id),
        output_table=Table(name="user", metadata=Metadata(schema="OwsHQ"), conn_id=bq_conn_id),
        if_exists="replace",
        use_native_support=True
    )

    stripe_parquet = aql.load_file(
        task_id="stripe_parquet",
        input_file=File(path=landing_zone_path + "mongodb-atlas/stripe/", filetype=FileType.PARQUET, conn_id=source_gcs_conn_id),
        output_table=Table(name="stripe", metadata=Metadata(schema="OwsHQ"), conn_id=bq_conn_id),
        if_exists="replace",
        use_native_support=True
    )

    # TODO Task Dependencies
    start >> trigger_airbyte_sync_atlas_gcs >> [user_parquet, stripe_parquet] >> end


# TODO DAG Instantiation
dag = init()
