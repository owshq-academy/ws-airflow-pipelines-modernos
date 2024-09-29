# import libraries
import os
import json
from dotenv import load_dotenv
from objects.musics import Musics
from google.cloud import pubsub_v1

# create sa [service account]
# svc-py-pub-sub@silver-charmer-243611.iam.gserviceaccount.com
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "silver-charmer-243611-svc-py-pub-sub.json"

# get env
load_dotenv()

# load variables
pub_sub_project = os.getenv("PUB_SUB_PROJECT")
pub_sub_topic = os.getenv("PUB_SUB_TOPIC")
size = os.getenv("SIZE")


# class to insert into datastore
class PubSub(object):

    @staticmethod
    def list_topics():
        # list available topics
        publisher = pubsub_v1.PublisherClient()
        project_path = f"projects/{pub_sub_project}"

        # for loop to list topics
        for topic in publisher.list_topics(request={"project": project_path}):
            print(topic)

    @staticmethod
    def push_payload():
        # set publisher
        # select topic and project
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(pub_sub_project, pub_sub_topic)

        # retrieve data from objects - music data
        # cast into json as utf-8 format
        get_data = Musics().get_multiple_rows(gen_dt_rows=size)
        print(get_data)

        # for loop to insert all data
        for data in get_data:
            # json with utf-8
            publish_events = json.dumps(data).encode("utf-8")
            print(publish_events)

            # publish events
            # retrieve id
            future = publisher.publish(topic_path, publish_events)
            print(future.result())