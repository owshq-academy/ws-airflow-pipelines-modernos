# import libraries
import os
from dotenv import load_dotenv

# get env
load_dotenv()

# load variables
kafka_client_id_json = os.getenv("KAFKA_CLIENT_ID_JSON")
hdinsight_kafka_bootstrap_server = os.getenv("HDINSIGHT_KAFKA_BOOTSTRAP_SERVER")


# [json] = producer config
def producer_settings_json(broker):

    json = {
        'client.id': kafka_client_id_json,
        'bootstrap.servers': broker,
        "batch.num.messages": 100000,
        "linger.ms": 1000
        }

    # return data
    return dict(json)
