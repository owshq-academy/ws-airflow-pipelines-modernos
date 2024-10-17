"""
Name:
Apache Spark Ingestion Action using Spark Pool

Description:
This code is responsible for triggering a custom pyspark job on a spark pool receiving metadata information from a trigger DAG.
It receives a context params from the trigger DAG and decouples the json payload to get the required parameters to build the pyspark job spec.
It creates a DAG dynamically based on the source of the data ingestion. The DAG loops through the list of tasks and creates a DAG for each task
registered on the metadata repository.

Pre-requisites:
- Install the following packages on the environment
apache-airflow-providers-microsoft-azure==8.0.0

- Create the service bus connection on Airflow azure_service_bus_ingestion_zone
Connection Type = Azure Service Bus
Connection String = <endpoint>

- Create the Synapse connection on Airflow azure_synapse_ingestion_zone_a_firstpool
Connection Type = Azure Synapse
Synapse Workspace URL = https://synapseworkspacename.dev.azuresynapse.net
Client ID = <service principal client id>
Secret = <service principal secret>
Tenant ID = <tenant id>
Subscription ID = <subscription id>

- Check if the variable spark_ingestion exists on Airflow otherwise it will fail to run
- Check if each task under spark_ingestion has it own variable otherwise it will fail to run
"""

import logging
import json

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from airflow.providers.microsoft.azure.hooks.asb import MessageHook, ServiceBusMessage
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunSparkBatchOperator

logger = logging.getLogger(__name__)
doc_md = """
### Apache Spark Ingestion Action using Spark Pool  
execute a custom pyspark job on a spark pool receiving metadata information from a trigger DAG.

#### Requirements:
- Copy additional Spark files to the storage account associated with the Spark cluster. 
Any additional file that may be added as part of the ingestion action needs
to be uploaded to the same container.

- Configure the Azure Synapse Analytics & Service Bus connection on Airflow. 
These connections will be needed to work transparently with the operator.

#### Notes:
- This DAG receives a context params from the trigger DAG and decouples the json payload 
to get the required parameters to build the pyspark job spec.

- Create or use an existing Spark Pool cluster to run the job. The AzureSynapseRunSparkBatchOperator receives
additional files and init parameters to execute.

- Azure ServiceBus is used to send the status of the job that will trigger Azure Function logic to handle.
"""

default_args = {
    "owner": "pythian",
    "retries": 0,
    "retry_delay": 0
}


@task(task_id="get_batch_metadata", multiple_outputs=True, retries=1)
def get_batch_metadata(dag_name):
    """
    Retrieve metadata from context sent by the trigger DAG function. This function decouples the json payload
    to get the required parameters to build the required spec to run the DAG.

    returns: dict
    """

    context = get_current_context()
    batch = context["params"]

    batch_id = batch["batch_id"]
    task_id = batch["task"]["task_id"]
    job_id = batch["job_id"]
    job_instance_id = batch["job_instance_id"]

    source_dataset = batch["task"]["task_parameters"]["source_code"]
    destination_dataset = batch["task"]["task_parameters"]["destination_code"]

    connection_id = batch["batch_parameters"]["runtime_parameters"]["batch_runner"]["connection_id"]
    cluster_name = batch["batch_parameters"]["runtime_parameters"]["batch_runner"]["name"]
    domain = batch["task"]["task_parameters"]["source_datasets"][source_dataset]["domain"]
    dataset_name = batch["task"]["task_parameters"]["source_datasets"][source_dataset]["dataset_name"]
    app_file_location = batch["batch_parameters"]["runtime_parameters"]["source_urls"]["app_dfs_url"]
    source_file_location = batch["batch_parameters"]["runtime_parameters"]["source_urls"]["data_dfs_url"]
    source_file_schema = batch["task"]["task_parameters"]["source_datasets"][source_dataset]["schema"]
    source_file_parameters = batch["task"]["task_parameters"]["source_datasets"][source_dataset]["file_parameters"]
    source_file_format = batch["task"]["task_parameters"]["source_datasets"][source_dataset]["file_parameters"]["file_format"]
    destination_data_dfs_url = batch["batch_parameters"]["runtime_parameters"]["destination_urls"]["data_dfs_url"]
    destination_file_schema = json.dumps(batch["task"]["task_parameters"]["destination_datasets"][destination_dataset]["schema"])
    destination_file_format = batch["task"]["task_parameters"]["destination_datasets"][destination_dataset]["file_type"]
    destination_table_parameters = json.dumps(batch["task"]["task_parameters"]["destination_datasets"][destination_dataset]["table_parameters"])

    if "spark_job_config" in batch["task"]["task_parameters"]:
        num_executors = batch["task"]["task_parameters"]["spark_job_config"].get(
            "numExecutors", 2)
        executor_cores = batch["task"]["task_parameters"]["spark_job_config"].get(
            "executorCores", 1)
        executor_memory = batch["task"]["task_parameters"]["spark_job_config"].get(
            "executorMemory", "8g")
        driver_cores = batch["task"]["task_parameters"]["spark_job_config"].get(
            "driverCores", 1)
        driver_memory = batch["task"]["task_parameters"]["spark_job_config"].get(
            "driverMemory", "8g")
    else:
        num_executors = 2
        executor_cores = 1
        executor_memory = "8g"
        driver_cores = 1
        driver_memory = "8g"

    wait_for_success_flag = batch["task"]["task_parameters"]["wait_for_success_flag"]

    if wait_for_success_flag is True or wait_for_success_flag == "True":
        file_extension = source_file_format.lower()
        source_file_location = f"{source_file_location}*.{file_extension}"

    log_dict = {
        "log_source": "job_orchestrator",
        "log_name": "job_orchestrator",
        "log_event": "process_batch",
        "batch_id": batch_id,
        "job_id": job_id,
        "job_instance_id": job_instance_id,
        "task_id": task_id,
        "msg": None
    }

    pyspark_job_payload = {
        "name": "SparkIngestAction",
        "file": f"{app_file_location}spark/action/ingestion/spark_ingestion_action.py",
        "args": [
            batch_id,
            domain,
            dataset_name,
            source_file_location,
            source_file_schema,
            source_file_parameters,
            source_file_format,
            destination_data_dfs_url,
            destination_file_schema,
            destination_file_format,
            destination_table_parameters
        ],
        "jars": [],
        "pyFiles": [
            f"{app_file_location}spark/action/ingestion/logger.py",
            f"{app_file_location}spark/action/ingestion/commons.py",
            f"{app_file_location}spark/action/ingestion/udf.py"
        ],
        "files": [],
        "numExecutors": num_executors,
        "executorCores": executor_cores,
        "executorMemory": executor_memory,
        "driverCores": driver_cores,
        "driverMemory": driver_memory
    }

    log_dict["msg"] = f"pyspark_job_payload: {pyspark_job_payload}"
    logger.info(log_dict)

    Variable.set(key=dag_name, value=json.dumps(pyspark_job_payload))

    return {
        "connection_id": connection_id,
        "cluster_name": cluster_name
    }


@task(task_id="set_batch_task", retries=1)
def set_batch_task(status_type):
    """
    Set the stages of the batch task to running, success or failed. The status of the task is sent to the
    Azure Service Bus topic to be handled by the Azure Function.

    returns: dict
    """

    context = get_current_context()
    batch = context["params"]

    connection_id = batch["batch_parameters"]["runtime_parameters"]["output_topic"]["connection_id"]
    output_topic = batch["batch_parameters"]["runtime_parameters"]["output_topic"]["name"]

    set_batch_metadata = {
        "batch_status": status_type,
        "batch_id": batch["batch_id"],
        "destination_urls": {
            "data_blob_url": batch["batch_parameters"]["runtime_parameters"]["destination_urls"]["data_blob_url"],
            "data_dfs_url": batch["batch_parameters"]["runtime_parameters"]["destination_urls"]["data_dfs_url"]
        },
        "status_timestamp": str(datetime.now())
    }

    mh = MessageHook(connection_id)
    asb_client = mh.get_conn()
    sender = asb_client.get_topic_sender(topic_name=output_topic)
    message = ServiceBusMessage(json.dumps(set_batch_metadata))
    sender.send_messages(message)

    return set_batch_metadata


def create_spark_dag(source):
    @dag(
        dag_id=f"{source}",
        doc_md=doc_md,
        start_date=datetime(2023, 10, 2),
        max_active_runs=1,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
        render_template_as_native_obj=True
    )
    def dag_spark_ingestion_template():
        """
        Build the DAG dynamically based on the source of the data ingestion.

        How does it work?
        - DAGs are created dynamically based on the variable named as the task code from metadata api.
        - variable needs to contain a list of dag names to be created based in a specific action code for e.g. spark_ingestion
        - dag name is composed of task code + action code for e.g. logistics_hourly_distances_spark_ingestion
        - variable is used to maintain metadata for a specific task
        - DAG loops through the list of tasks and creates a DAG for each task
        - Whenever the variable list is updated with a new DAG, the DAG will be created automatically

        Tasks taken by this DAG template:
        - get_batch_metadata = gets the metadata from the trigger DAG
        - spark_pool_action = triggers the spark pool action
        - set_batch_success = set the batch status to success
        - set_batch_failure = set the batch status to failure
        """

        get_dag_name = f"{source}"

        get_batch_metadata_payload = get_batch_metadata(get_dag_name)
        logger.info(f"metadata: {get_batch_metadata_payload}")

        connection_id = get_batch_metadata_payload["connection_id"]
        cluster_name = get_batch_metadata_payload["cluster_name"]

        pyspark_job_payload = json.loads(Variable.get(key=f"{get_dag_name}"))

        spark_pool_action = AzureSynapseRunSparkBatchOperator(
            task_id="spark_pool_action",
            azure_synapse_conn_id=connection_id,
            spark_pool=cluster_name,
            payload=pyspark_job_payload,
            wait_for_termination=True,
            timeout=1800,
            check_interval=60
        )

        success = EmptyOperator(
            task_id="success", trigger_rule=TriggerRule.ALL_SUCCESS)
        set_batch_success = set_batch_task(status_type="success")

        failure = EmptyOperator(
            task_id="failure", trigger_rule=TriggerRule.ALL_FAILED)
        set_batch_failure = set_batch_task(status_type="failure")

        get_batch_metadata_payload >> spark_pool_action >> success >> set_batch_success
        get_batch_metadata_payload >> spark_pool_action >> failure >> set_batch_failure

    generated_dag = dag_spark_ingestion_template()

    return generated_dag


@dag(schedule_interval=None, start_date=datetime(2023, 10, 2), )
def spark_ingestion():
    """
    Retrieves the list of tasks from the variable spark_ingestion {action code} and creates a DAG for each task.

    returns: DAG
    """

    get_spark_val = json.loads(Variable.get(key="spark_ingestion"))

    if isinstance(get_spark_val, str):
        get_spark_var_dict = json.loads(get_spark_val)
    else:
        get_spark_var_dict = get_spark_val

    tasks = get_spark_var_dict.get("tasks", [])

    for task_info in tasks:
        task_name = next(iter(task_info))

        globals()[task_name] = create_spark_dag(task_name)


spark_ingestion()
