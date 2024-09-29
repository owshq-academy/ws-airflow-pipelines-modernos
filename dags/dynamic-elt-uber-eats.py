from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from astro.sql.table import Table, Metadata
from astro.sql.operators.load_file import LoadFileOperator as LoadFile
from astro.files import get_file_list

GCS_CONN_ID = "google_cloud_default"
BIGQUERY_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="dynamic-elt-uber-eats",
    start_date=datetime(2024, 9, 3),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 'gcs']
)
def load_data():

    split_files_by_metadata = EmptyOperator(task_id="split_files_by_metadata")
    finish_loading_to_dw_process = EmptyOperator(task_id="finish_loading_to_dw_process")

    sources = [
        {'entity': 'users', 'url': 'gs://owshq-uber-eats-files/com.owshq.data/mssql/users/'},
        {'entity': 'drivers', 'url': 'gs://owshq-uber-eats-files/com.owshq.data/postgres/drivers/'}
    ]

    for source in sources:

        with TaskGroup(group_id=source["entity"]) as task_group_entities:

            load_from_storage_dw = LoadFile.partial(
                task_id="load_from_storage_dw",
                output_table=Table(
                    name=source["entity"],
                    metadata=Metadata(
                        schema="OwsHQ",
                    ),
                    conn_id=BIGQUERY_CONN_ID
                ),
                if_exists="replace",
                use_native_support=True,
                columns_names_capitalization="original"
            ).expand(input_file=get_file_list(path=source["url"], conn_id=GCS_CONN_ID))

            load_from_storage_dw

        split_files_by_metadata >> task_group_entities >> finish_loading_to_dw_process


dag = load_data()
