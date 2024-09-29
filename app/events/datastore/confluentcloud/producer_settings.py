# import libraries
import os
from dotenv import load_dotenv

# get env
load_dotenv()

# load variables
kafka_client_id_json = os.getenv("KAFKA_CLIENT_ID_JSON")
confluent_cloud_bootstrap_server = os.getenv("CONFLUENT_CLOUD_BOOTSTRAP_SERVER")
confluent_cloud_security_protocol = os.getenv("CONFLUENT_CLOUD_SECURITY_PROTOCOL")
confluent_cloud_sasl_mechanisms = os.getenv("CONFLUENT_CLOUD_SASL_MECHANISMS")
confluent_cloud_sasl_username = os.getenv("CONFLUENT_CLOUD_SASL_USERNAME")
confluent_cloud_sasl_password = os.getenv("CONFLUENT_CLOUD_SASL_PASSWORD")


# [json] = producer config
def producer_settings_json(broker):

    json = {
        'client.id': kafka_client_id_json,
        'bootstrap.servers': broker,
        'security.protocol': confluent_cloud_security_protocol,
        'sasl.mechanism': confluent_cloud_sasl_mechanisms,
        'sasl.username': confluent_cloud_sasl_username,
        'sasl.password': confluent_cloud_sasl_password
        }

    # return data
    return dict(json)