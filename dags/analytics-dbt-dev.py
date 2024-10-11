"""
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

default_dbt_root_path = Path(__file__).parent / "dbt"
dbt_root_path = Path(os.getenv("DBT_ROOT_PATH", default_dbt_root_path))

profile_config = ProfileConfig(
    profile_name="silver-charmer-243611",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": "silver-charmer-243611",
            "dataset": "OwsHQ"
        },
    ),
)


default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="analytics-dbt-dev",
    start_date=datetime(2024, 10, 1),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'cosmos', 'gcp', 'bq', 'dbt']
)
def dbt_analytics_project() -> None:
    """
    Turns a dbt project into a TaskGroup with a profile mapping.
    """

    start = EmptyOperator(task_id="start")

    analytics = DbtTaskGroup(
        project_config=ProjectConfig(
            (dbt_root_path / "analytics").as_posix()
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True
        }
    )

    end = EmptyOperator(task_id="end")

    start >> analytics >> end


dbt_analytics_project()
