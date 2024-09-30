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

# TODO imports
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

# TODO default arguments
default_args = {
    'owner': 'luan moreno m. maciel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# TODO DAG definition
@dag(
    dag_id='dag-structure',
    start_date=datetime(2024, 9, 30),
    max_active_runs=1,
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=['first', 'dag']
)
def init():

    # TODO task declaration
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # TODO task dependencies
    start >> end


# TODO DAG instantiation
dag = init()
