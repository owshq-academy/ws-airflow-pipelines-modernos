import json
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta

users_dataset = Dataset("gs://owshq-uber-eats-files/users/")
orders_dataset = Dataset("gs://owshq-uber-eats-files/orders/")

default_args = {
    "owner": "luan moreno m. maciel",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id="consumer-users-orders-json-files",
    start_date=datetime(2024, 9, 24),
    max_active_runs=1,
    schedule=[users_dataset, orders_dataset],
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'elt', 'gcs', 'consumer', 'datasets', 'orders', 'users']
)
def init():
    start = EmptyOperator(task_id='start')

    @task
    def process_users_data():
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket = "owshq-uber-eats-files"
        prefix = "users/"

        files = gcs_hook.list(bucket_name=bucket, prefix=prefix)
        if not files:
            print(f"No files found in {bucket}/{prefix}")
            return

        def get_file_update_time(filename):
            metadata = gcs_hook.get_metadata(bucket, filename)
            if metadata:
                return metadata.get('updated', '')
            return ''

        latest_file = max(files, key=get_file_update_time)

        try:
            file_content = gcs_hook.read(bucket_name=bucket, object_name=latest_file)
            users_data = json.loads(file_content)

            user_count = len(users_data)
            print(f"Processed {user_count} users from {latest_file}")
        except Exception as e:
            print(f"Error processing file {latest_file}: {str(e)}")

    @task
    def process_orders_data():
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket = "owshq-uber-eats-files"
        prefix = "orders/"

        files = gcs_hook.list(bucket_name=bucket, prefix=prefix)
        if not files:
            print(f"No files found in {bucket}/{prefix}")
            return

        def get_file_update_time(filename):
            metadata = gcs_hook.get_metadata(bucket, filename)
            if metadata:
                return metadata.get('updated', '')
            return ''

        latest_file = max(files, key=get_file_update_time)

        try:
            file_content = gcs_hook.read(bucket_name=bucket, object_name=latest_file)
            orders_data = json.loads(file_content)

            total_revenue = sum(order['total_amount'] for order in orders_data)
            print(f"Processed orders from {latest_file}. Total revenue: {total_revenue}")
        except Exception as e:
            print(f"Error processing file {latest_file}: {str(e)}")

    end = EmptyOperator(task_id='end')

    start >> [process_users_data(), process_orders_data()] >> end


dag = init()
