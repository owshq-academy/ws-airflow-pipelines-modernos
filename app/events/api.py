# import libraries
import os
from datastore.kafka.kafka_json import Kafka
from objects.events import Events
from objects.rides import Rides
from fastapi import FastAPI
from dotenv import load_dotenv

# load env
load_dotenv()

# set variables
kafka_broker = os.getenv("KAFKA_BOOTSTRAP_SERVER")
user_events_json_topic = os.getenv("KAFKA_TOPIC_USER_EVENTS_JSON")
flight_events_json_topic = os.getenv("KAFKA_TOPIC_FLIGHT_EVENTS_JSON")
ride_events_json_topic = os.getenv("KAFKA_TOPIC_RIDE_EVENTS_JSON")

# init fast api
# uvicorn api:app --reload
# http://127.0.0.1:8000/docs
app = FastAPI()


# post [calls]
@app.post("/kafka/user_events/{get_dt_rows}")
def post_user_events(get_dt_rows: int):
    return Kafka().json_producer(broker=kafka_broker, object_name=Events().get_user_events(get_dt_rows), kafka_topic=user_events_json_topic)


@app.post("/kafka/flight_events/{get_dt_rows}")
def post_flight_events(get_dt_rows: int):
    return Kafka().json_producer(broker=kafka_broker, object_name=Events().get_flight_events(get_dt_rows), kafka_topic=flight_events_json_topic)


@app.post("/kafka/ride_events/{get_dt_rows}")
def post_ride_events(get_dt_rows: int):
    return Kafka().json_producer(broker=kafka_broker, object_name=Rides().get_multiple_rows(get_dt_rows), kafka_topic=ride_events_json_topic)




