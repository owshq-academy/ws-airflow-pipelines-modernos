from airflow.decorators import dag
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 17),
    catchup=False,
    tags=['sensor', 'gcs']
)
def gcs_file_sensor():

    wait_users_file = GCSObjectsWithPrefixExistenceSensor(
        task_id='wait_users_file',
        bucket='owshq-airbyte-ingestion',
        prefix='mongodb-atlas/users/',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=30,  # TODO check every 30 seconds
        timeout=60 * 60 * 24,  # TODO timeout after 24 hours
        mode='poke'
    )

    chain(wait_users_file)


dag = gcs_file_sensor()
