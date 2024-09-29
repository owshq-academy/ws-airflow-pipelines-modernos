# import libraries
import os
import time
import pandas as pd
from data_requests.api_requests import Requests
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# get env
load_dotenv()

# load variables
endpoint = os.getenv("COSMOSDB_ENDPOINT")
primarykey = os.getenv("COSMOSDB_PRIMARYKEY")
size = os.getenv("SIZE")

# pandas config
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# class to insert into datastore
class CosmosDB(object):

    @staticmethod
    def insert_documents_sql_api():
        # initialize the cosmos client
        # create database
        client = CosmosClient(endpoint, credential=primarykey)
        database_name = 'pythian'

        # try logic to create database
        try:
            database = client.create_database(database_name)
        except exceptions.CosmosResourceExistsError:
            database = client.get_database_client(database_name)

        # create container
        # partitioned by user_id
        # enable analytical storage
        # container level enablement
        container_name = 'users'

        # try logic to create the container
        # The 3 options for the analytical_storage_ttl parameter are:
        # 1) 0 or null or not informed (not enabled).
        # 2) -1 (the data will be stored in analytical store infinitely).
        # 3) any other number is the actual ttl, in seconds.
        try:
            container = database.create_container(id=container_name, partition_key=PartitionKey(path="/user_id"), analytical_storage_ttl=-1)
        except exceptions.CosmosResourceExistsError:
            container = database.get_container_client(container_name)
        except exceptions.CosmosHttpResponseError:
            raise

        # id = users
        # database = pythian
        # ru/s = 400
        q_database = client.get_database_client(database_name)
        q_container = database.get_container_client(container_name)

        # return database and container info
        print(q_database)
        print(q_container)

        # query containers available
        database = client.get_database_client(database_name)
        for container in database.list_containers():
            print(container['id'])

        # retrieve users data to ingest into cosmosdb
        # get data from using api request
        params = {'size': size}
        url_get_user = 'https://random-data-api.com/api/users/random_user'
        dt_user = Requests.api_get_request(url=url_get_user, params=params)

        # convert python list (dict)
        # use pandas dataframe to ease the insert of the data
        # add [user_id] into dataframe
        # add [dt_current_timestamp] into dataframe
        pd_df_user = pd.DataFrame.from_dict(dt_user)
        pd_df_user['user_id'] = Requests().gen_user_id()
        pd_df_user['dt_current_timestamp'] = Requests().gen_timestamp()

        # cast from int to string to ingest into cosmosdb container
        # using pandas dataframe to insert
        # using astype to cast to string
        pd_df_user['id'] = pd_df_user['id'].values.astype(str)
        pd_df_user['user_id'] = pd_df_user['user_id'].values.astype(str)
        pd_df_user['dt_current_timestamp'] = pd_df_user['dt_current_timestamp'].values.astype(str)

        # [START upsert_items]
        # data written in a loop
        # using SQL API
        start = time.time()
        for i in range(0, pd_df_user.shape[0]):
            # create a dictionary for the selected row
            data_dict = dict(pd_df_user.iloc[i, :])
            # connect into database and get container name
            # upsert items into container
            container = database.get_container_client(container_name)
            container.upsert_item(data_dict)
            print(data_dict)

        print(f"total of documents ingested: {size}")
        print(f"time taken to ingest documents [secs]: {round(time.time() - start, 2)}")
        # [END upsert_items]

        # query total of documents written into sql api per [batch]
        # verify amount of documents into data store
        query_count = "SELECT VALUE COUNT(1) FROM users"
        items = list(container.query_items(query=query_count, enable_cross_partition_query=True))
        request_charge = container.client_connection.last_response_headers['x-ms-request-charge']

        print('query returned {0} item. operation consumed {1} request units'.format(len(items), request_charge))
        print(f"total documents into cosmosdb: {items}")
        print(f"documents ingested successfully into sql api")