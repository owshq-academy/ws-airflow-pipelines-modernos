from airflow import DAG
import dagfactory

config_file = "/usr/local/airflow/dags/dag_factory.yml"
dag_factory = dagfactory.DagFactory(config_file)

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
