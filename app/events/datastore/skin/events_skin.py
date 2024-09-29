# import libraries
import http
import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from data_requests.api_requests import Requests

# get env
load_dotenv()

# load variables
size = os.getenv("SIZE")

# set up parameters to request from api call
params = {'size': size}


# class to insert into datastore
class Skin(object):

    @staticmethod
    def generate_events():

        # generate data from api
        selected_url = 'https://random-data-api.com/api/users/random_user'

        # get request [api] to store in a variable
        # using method get to retrieve data
        dt_data = Requests.api_get_request(url=selected_url, params=params)

        # convert python list (dict)
        # use pandas dataframe to ease the insert of the data
        pd_df_data = pd.DataFrame.from_dict(dt_data)

        # add [user_id] into dataframe
        # add [dt_current_timestamp] into dataframe
        pd_df_data['user_id'] = Requests().gen_user_id()
        pd_df_data['dt_current_timestamp'] = Requests().gen_timestamp()

        # export to json format
        # set records and utf-8

        # TODO change to kong url ~ [gateway]
        # skin_url = "http://localhost:5757/json"
        skin_url = "http://20.85.8.77/json"

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        json_data = pd_df_data.to_json(orient="records").encode('utf-8')

        # request to skin service to control ingestion
        # init clock time to measure
        start = time.time()

        # TODO change auth method - api key
        # req_skin_svc = requests.post(skin_url, data=json_data, headers=headers)
        req_skin_svc = requests.post(skin_url+'?apikey=05FPrYc95j458S8BEactii2qtbR7l1z7', data=json_data, headers=headers)

        if req_skin_svc.status_code == http.HTTPStatus.UNAUTHORIZED or req_skin_svc.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
            print(req_skin_svc.status_code)
            print(req_skin_svc.headers)
            print(req_skin_svc.text)

        else:
            print(req_skin_svc.status_code)
            print(req_skin_svc.headers)
            print(f"amount of events sent to api: {size}")
            print(f"time taken to ingest into apache kafka: {round(time.time() - start, 2)} secs")



