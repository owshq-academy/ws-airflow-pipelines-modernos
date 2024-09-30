"""
Hook to connect to MongoDB Atlas and retrieve a document from a collection.
"""

# TODO Libraries
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

# TODO Connections & Variables
mongodb_atlas_conn_id = "mongodb_default"

# TODO Default Arguments
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TODO DAG Definition
@dag(
    dag_id="dag-first-hook",
    start_date=datetime(2024, 9, 24),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=5),
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'elt', 'gcs', 'mongodb']
)
def init():

    # TODO Tasks Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # TODO Retrieve users collection from MongoDB
    @task
    def get_users():
        hook = MongoHook(conn_id=mongodb_atlas_conn_id)

        database = "owshq"
        collection = "users"

        query = {"cpf": "596.175.900-85"}

        try:
            conn = hook.get_conn()
            db = conn[database]
            coll = db[collection]

            count = coll.count_documents(query)

            data = hook.find(
                mongo_db=database,
                mongo_collection=collection,
                query=query
            )

            documents = list(data)
            doc_count = len(documents)

            if doc_count == 0:
                print("No documents found.")
                return None
            elif doc_count > 1:
                print(f"Warning: Found {doc_count} documents. Returning the first one.")

            document = json.loads(json.dumps(documents[0], default=str))
            print(f"Retrieved document: {document}")

            return document

        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise

    users_collection = get_users()

    # TODO Task Dependencies
    start >> users_collection >> end


# TODO DAG Instantiation
dag = init()