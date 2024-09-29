# import libraries
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from data_requests.api_requests import Requests
from datetime import datetime

# get env
load_dotenv()

# load variables
size = os.getenv("SIZE")
get_dt_rows = os.getenv("EVENTS")
blob_storage_conn_str = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
container_landing = os.getenv("LANDING_CONTAINER_NAME")

# set up parameters to request from api call
params = {'size': size}


# class to insert into datastore
class BlobStorage(object):

    @staticmethod
    def write_into_landing_zone_json(entity):
        # set correct file name
        # file name + datetime
        # example = beer_2021_04_07_16_21
        year = datetime.today().year
        month = datetime.today().month
        day = datetime.today().day
        hour = datetime.today().hour
        minute = datetime.today().minute
        second = datetime.today().second
        file_name = entity + f'/{entity}_{year}_{month}_{day}_{hour}_{minute}_{second}.json'

        # init url requests variables
        # creating a dictionary of available objects = entities
        # input to select which file to process
        url_requests_api = {
                            'user': 'https://random-data-api.com/api/users/random_user',
                            'restaurant': 'https://random-data-api.com/api/restaurant/random_restaurant',
                            'vehicle': 'https://random-data-api.com/api/vehicle/random_vehicle',
                            'stripe': 'https://random-data-api.com/api/stripe/random_stripe',
                            'google_auth': 'https://random-data-api.com/api/omniauth/google_get',
                            'facebook_auth': 'https://random-data-api.com/api/omniauth/facebook_get',
                            'twitter_auth': 'https://random-data-api.com/api/omniauth/twitter_get',
                            'linkedin_auth': 'https://random-data-api.com/api/omniauth/linkedin_get',
                            'github_auth': 'https://random-data-api.com/api/omniauth/github_get',
                            'apple_auth': 'https://random-data-api.com/api/omniauth/apple_get',
                            'bank': 'https://random-data-api.com/api/bank/random_bank',
                            'credit_card': 'https://random-data-api.com/api/business_credit_card/random_card',
                            'subscription': 'https://random-data-api.com/api/subscription/random_subscription',
                            'company': 'https://random-data-api.com/api/company/random_company',
                            'commerce': 'https://random-data-api.com/api/commerce/random_commerce',
                            'computer': 'https://random-data-api.com/api/computer/random_computer',
                            'device': 'https://random-data-api.com/api/device/random_device',
                            'beer': 'https://random-data-api.com/api/beer/random_beer',
                            'coffee': 'https://random-data-api.com/api/coffee/random_coffee',
                            'food': 'https://random-data-api.com/api/food/random_food',
                            'dessert': 'https://random-data-api.com/api/dessert/random_dessert'
                            }
        selected_url = url_requests_api[entity]

        # get request [api] to store in a variable
        # using method get to retrieve data
        dt_data = Requests.api_get_request(url=selected_url, params=params)
        print(file_name)
        # print(dt_data)

        # convert python list (dict)
        # use pandas dataframe to ease the insert of the data
        pd_df_data = pd.DataFrame.from_dict(dt_data)

        # add [user_id] into dataframe
        # add [dt_current_timestamp] into dataframe
        pd_df_data['user_id'] = Requests().gen_user_id()
        pd_df_data['dt_current_timestamp'] = Requests().gen_timestamp()

        # write into blob storage [pandas dataframe] to json file
        # select entity to write into blob storage
        # cast data output into json with utf-8
        json_data = pd_df_data.to_json(orient="records").encode('utf-8')

        # instantiate a new blob service client using a connection string
        # instantiate a new container client
        blob_service_client = BlobServiceClient.from_connection_string(blob_storage_conn_str)
        container_client = blob_service_client.get_container_client(container_landing)

        # instantiate a new blob client
        # upload data into blob storage
        blob_client = container_client.get_blob_client(file_name)
        blob_client.upload_blob(json_data, blob_type="BlockBlob")

    @staticmethod
    def write_all():
        # list of all available urls to write
        # ingest dynamically by calling the first function
        urls_available = [
            'user',
            'restaurant',
            'vehicle',
            'stripe',
            'bank',
            'credit_card',
            'subscription',
            'company',
            'commerce',
            'computer',
            'device',
            'beer',
            'coffee',
            'food',
            'dessert']

        # init conditioner & counter
        count_list = len(urls_available)
        i = 0

        # loop to read all files within the received list
        # invoke function to go over xml files
        while i < count_list:

            # execute first function to ingest into minio storage
            # going over each item into the list
            # print(urls_available[i])
            BlobStorage().write_into_landing_zone_json(entity=urls_available[i])

            # finish count iterable
            i += 1
