"""
DAG Skeleton

- Imports
- Connections & Variables
- Default Arguments
- DAG Definition
- Task Declaration
- Task Dependencies
- DAG Instantiation
"""

# TODO Imports
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

# TODO Connections & Variables

# TODO Default Arguments
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TODO DAG Definition
@dag(
    dag_id="dag-structure",
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

    # TODO Task Dependencies
    start >> end


# TODO DAG Instantiation
dag = init()
