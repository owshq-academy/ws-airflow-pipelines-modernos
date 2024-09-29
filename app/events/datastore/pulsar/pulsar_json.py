# import libraries
import os
import json
import pulsar
from dotenv import load_dotenv

# get env
load_dotenv()

# load variables
pulsar_broker = os.getenv("PULSAR_BROKER")
pulsar_rides_topic = os.getenv("PULSAR_RIDES_TOPIC")


# class to insert into datastore
class Pulsar(object):

    # producer function
    @staticmethod
    def json_producer(object_name):

        # init producer settings
        # async send with callback
        client = pulsar.Client(pulsar_broker)
        producer = client.create_producer(topic=pulsar_rides_topic)

        # get object [dict] from objects
        # transform into tuple to a multiple insert process
        events = object_name
        print(type(events))
        print(events)

        # loop through data
        # prepare message
        for event in events:

            # encode message
            data = json.dumps(event).encode('utf-8')

            # send event to broker
            # current mode = sync
            # blocks until the message is acknowledged
            producer.send(data)
            print(data)

        # close client
        client.close()