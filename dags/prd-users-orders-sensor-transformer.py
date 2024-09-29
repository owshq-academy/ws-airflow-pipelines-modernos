"""
DAG Skeleton

- Imports [1]
- Connections & Variables [2]
- DataSets [3]
- Default Arguments [4]
- DAG Definition [5]
- Task Declaration
- Task Dependencies
- DAG Instantiation
"""

# TODO Imports
import json
import tempfile
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.datasets import Dataset
from typing import List, Dict

# TODO Connections & Variables
GCS_CONN_ID = "google_cloud_default"
BIGQUERY_CONN_ID = "google_cloud_default"
BUCKET_SOURCE = "owshq-uber-eats-files"

# TODO DataSets [Outlets]
users_dataset = Dataset(f"gs://{BUCKET_SOURCE}/users/")
orders_dataset = Dataset(f"gs://{BUCKET_SOURCE}/orders/")

# TODO Default Arguments
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TODO DAG Definition
@dag(
    dag_id="prd-users-orders-sensor-transformer",
    start_date=datetime(2024, 9, 24),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=5),
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'elt', 'gcs', 'files']
)
def init():

    # TODO Tasks Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # TODO Task [1]
    get_mongodb_users_json = GCSObjectsWithPrefixExistenceSensor(
        task_id="get_mongodb_users_json",
        bucket="owshq-uber-eats-files",
        prefix="com.owshq.data/mongodb/users/",
        google_cloud_conn_id=GCS_CONN_ID,
        timeout=3600,
        poke_interval=30,
        deferrable=True
    )

    get_kafka_orders_json = GCSObjectsWithPrefixExistenceSensor(
        task_id="get_kafka_orders_json",
        bucket="owshq-uber-eats-files",
        prefix="com.owshq.data/kafka/orders/",
        google_cloud_conn_id=GCS_CONN_ID,
        timeout=3600,
        poke_interval=30,
        deferrable=True
    )

    @task
    def list_files(bucket: str, prefix: str) -> List[str]:
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        files = gcs_hook.list(bucket_name=bucket, prefix=prefix)
        return [f for f in files if f.endswith('.json')]

    @task
    def transform_user(file_path: str) -> List[Dict]:
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        with tempfile.NamedTemporaryFile() as temp_file:
            gcs_hook.download(bucket_name="owshq-uber-eats-files", object_name=file_path, filename=temp_file.name)
            with open(temp_file.name, 'r') as file:
                users_data = json.load(file)

        transformed_users = []
        for user_data in users_data:
            transformed_user = {
                "id": user_data["id"],
                "full_name": f"{user_data['first_name']} {user_data['last_name']}",
                "email": user_data["email"],
                "birth_year": int(user_data["date_of_birth"].split("-")[0]),
                "city": user_data["address"]["city"],
                "country": user_data["address"]["country"],
                "subscription_status": user_data["subscription"]["status"]
            }
            transformed_users.append(transformed_user)
            print(f"Transformed User: {transformed_user}")

        return transformed_users

    @task
    def transform_order(file_path: str) -> List[Dict]:
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        with tempfile.NamedTemporaryFile() as temp_file:
            gcs_hook.download(bucket_name="owshq-uber-eats-files", object_name=file_path, filename=temp_file.name)
            with open(temp_file.name, 'r') as file:
                orders_data = json.load(file)

        transformed_orders = []
        for order_data in orders_data:
            transformed_order = {
                "order_id": order_data["order_id"],
                "user_id": order_data["user_id"],
                "restaurant_cnpj": order_data["restaurant_cnpj"],
                "order_date": order_data["order_date"],
                "total_amount": float(order_data["total_amount"]),
                "order_year": int(order_data["order_date"].split("-")[0]),
                "order_month": int(order_data["order_date"].split("-")[1])
            }
            transformed_orders.append(transformed_order)
            print(f"Transformed Order: {transformed_order}")

        return transformed_orders

    @task(outlets=[users_dataset])
    def write_users_to_gcs(data: List[List[Dict]]) -> None:
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        object_name = f"users/users_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

        flattened_data = [item for sublist in data for item in sublist]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as temp_file:
            json.dump(flattened_data, temp_file)
            temp_file.flush()
            gcs_hook.upload(
                bucket_name=BUCKET_SOURCE,
                object_name=object_name,
                filename=temp_file.name
            )
        print(f"Users data written to gs://{BUCKET_SOURCE}/{object_name}")

    @task(outlets=[orders_dataset])
    def write_orders_to_gcs(data: List[List[Dict]]) -> None:
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        object_name = f"orders/orders_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

        flattened_data = [item for sublist in data for item in sublist]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as temp_file:
            json.dump(flattened_data, temp_file)
            temp_file.flush()
            gcs_hook.upload(
                bucket_name=BUCKET_SOURCE,
                object_name=object_name,
                filename=temp_file.name
            )
        print(f"Orders data written to gs://{BUCKET_SOURCE}/{object_name}")

    @task_group(group_id='transform_users')
    def transform_users():
        user_files = list_files(bucket="owshq-uber-eats-files", prefix="com.owshq.data/mongodb/users/")
        transformed_users = transform_user.expand(file_path=user_files)
        write_users_to_gcs(transformed_users)

    @task_group(group_id='transform_orders')
    def transform_orders():
        order_files = list_files(bucket="owshq-uber-eats-files", prefix="com.owshq.data/kafka/orders/")
        transformed_orders = transform_order.expand(file_path=order_files)
        write_orders_to_gcs(transformed_orders)

    # TODO Task Dependencies
    start >> get_mongodb_users_json >> transform_users() >> end
    start >> get_kafka_orders_json >> transform_orders() >> end


# TODO DAG Instantiation
dag = init()