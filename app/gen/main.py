"""
This script defines the `BlobStorage` class, which is responsible for generating synthetic data,
storing it in a dictionary for later use, and uploading it to Azure Blob Storage. The script uses
the `Faker` library to generate realistic-looking data across multiple domains, including users,
customers, drivers, restaurants, menus, payments, and orders.

The script supports multiple data sources (`mssql`, `postgres`, `mongodb`, and `kafka`) and ensures
data consistency across these sources by storing intermediate data in `self.stored_data` and reusing
it in later stages. This approach guarantees that the data generated for `mssql`, `postgres`, and
`mongodb` can be correctly linked when generating orders in `kafka`.
"""

import os
import pandas as pd

from google.cloud import storage
from pymongo import MongoClient
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime
from src.objects import customers, orders, status, users, menu, payments, restaurants, drivers, credit_card, merchants, salesforce
from src.api import api_requests

load_dotenv()

current_directory = os.path.dirname(os.path.abspath(__file__))
gcs_key_file = os.getenv("GCP_KEY_FILE")
bucket_name = os.getenv("GCS_BUCKET_NAME")
mongo_uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB_NAME")

if gcs_key_file:
    full_key_file_path = os.path.join(current_directory, gcs_key_file)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = full_key_file_path

client = MongoClient(mongo_uri)
db = client[db_name]

users = users.Users()
customers = customers.Customers()
drivers = drivers.Drivers()
restaurants = restaurants.Restaurants()
menu = menu.Menu()
payments = payments.Payments()
credit_card = credit_card.CreditCard()
salesforce = salesforce.SalesForce()
merchants = merchants.Merchants()

gen_amount = 10000
api = api_requests.Requests()


class Storage(object):
    """
    This class is used to write data into the landing zone
    """

    def __init__(self, bucket_name):
        """
        Initialize the BlobStorage object.

        Args:
            bucket_name: The name of the GCS bucket.
        """

        self.bucket_name = bucket_name
        self.stored_data = {}

    @staticmethod
    def create_dataframe(dt, ds_type, is_cpf=False, is_cnpj=False, cpf_list=None, gen_user_id=True):
        """
        Create a dataframe based on the provided data and data source type.

        Args:
            dt: The data to create the dataframe from.
            ds_type: The type of the data source.
            is_cpf: Whether generates a cpf.
            gen_user_id: Whether to generate a user_id.

        Returns:
            tuple: A tuple containing the JSON-encoded dataframe and the data source type.
        """

        pd_df = pd.DataFrame(dt)

        if gen_user_id:
            user_ids = [api.gen_user_id() for _ in range(len(pd_df))]
            if len(user_ids) != len(pd_df):
                raise ValueError("Generated user IDs do not match DataFrame length")
            pd_df['user_id'] = user_ids

        pd_df['dt_current_timestamp'] = api.gen_timestamp()

        if is_cpf:
            if cpf_list:
                if len(cpf_list) < len(pd_df):
                    raise ValueError("cpf list is shorter than DataFrame length.")
                pd_df['cpf'] = cpf_list[:len(pd_df)]
            else:
                pd_df['cpf'] = [api.gen_cpf() for _ in range(len(pd_df))]

        if is_cnpj:
            pd_df['cnpj'] = [api.gen_cnpj() for _ in range(len(pd_df))]

        json_data = pd_df.to_json(orient="records").encode('utf-8')
        return json_data, ds_type

    def upload_blob(self, json_data, file_name):
        """Upload a blob to the specified GCS bucket.

        Args:
            json_data: The JSON data to upload.
            file_name: The name of the file to upload.
        """

        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json_data)

    def write_file(self, ds_type: str):
        """
        Write files based on the specified data source type.

        Args:
            ds_type: The type of the data source.
        """

        gen_cpf = api.gen_cpf()
        gen_cnpj = api.gen_cnpj()

        year, month, day, hour, minute, second = (
            datetime.now().strftime("%Y %m %d %H %M %S").split()
        )

        if ds_type == "mssql":
            dt_users = users.get_multiple_rows(gen_dt_rows=gen_amount)
            dt_credit_card = credit_card.get_multiple_rows(gen_dt_rows=gen_amount)

            users_json, ds_type = self.create_dataframe(dt_users, ds_type, is_cpf=gen_cpf)
            credit_card_json, ds_type = self.create_dataframe(dt_credit_card, ds_type)

            users_df = pd.read_json(StringIO(users_json.decode('utf-8')))
            self.stored_data['users'] = users_df.to_dict(orient='records')
            self.stored_data['credit_card'] = dt_credit_card

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            user_file_name = file_prefix + "/users" + "/" + timestamp
            self.upload_blob(users_json, user_file_name)

            credit_card_file_name = file_prefix + "/credit_card" + "/" + timestamp
            self.upload_blob(credit_card_json, credit_card_file_name)

            return user_file_name, credit_card_file_name

        elif ds_type == "postgres":
            dt_drivers = drivers.get_multiple_rows(gen_dt_rows=gen_amount)
            dt_restaurants = restaurants.get_multiple_rows(gen_dt_rows=gen_amount)

            driver_json, ds_type = self.create_dataframe(dt_drivers, ds_type, is_cpf=gen_cpf, gen_user_id=False)
            restaurants_json, ds_type = self.create_dataframe(dt_restaurants, ds_type, is_cnpj=gen_cnpj, gen_user_id=False)

            drivers_df = pd.read_json(StringIO(driver_json.decode('utf-8')))
            restaurants_df = pd.read_json(StringIO(restaurants_json.decode('utf-8')))
            self.stored_data['drivers'] = drivers_df.to_dict(orient='records')
            self.stored_data['restaurants'] = restaurants_df.to_dict(orient='records')

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            driver_file_name = file_prefix + "/drivers" + "/" + timestamp
            self.upload_blob(driver_json, driver_file_name)

            restaurants_file_name = file_prefix + "/restaurants" + "/" + timestamp
            self.upload_blob(restaurants_json, restaurants_file_name)

            return driver_file_name, restaurants_file_name

        elif ds_type == "mongodb":
            dt_users = customers.get_multiple_rows(gen_dt_rows=gen_amount)
            dt_payments = payments.get_multiple_rows(gen_dt_rows=gen_amount)
            dt_menu = menu.get_multiple_rows(gen_dt_rows=gen_amount)

            users_json, ds_type = self.create_dataframe(dt_users, ds_type, is_cpf=gen_cpf)
            payments_json, ds_type = self.create_dataframe(dt_payments, ds_type, gen_user_id=False)
            menu_json, ds_type = self.create_dataframe(dt_menu, ds_type, is_cnpj=gen_cnpj, gen_user_id=False)

            payments_df = pd.read_json(StringIO(payments_json.decode('utf-8')))
            menu_df = pd.read_json(StringIO(menu_json.decode('utf-8')))
            self.stored_data['payments'] = payments_df.to_dict(orient='records')
            self.stored_data['menu'] = menu_df.to_dict(orient='records')

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            users_file_name = file_prefix + "/users" + "/" + timestamp
            self.upload_blob(users_json, users_file_name)

            payments_file_name = file_prefix + "/payments" + "/" + timestamp
            self.upload_blob(payments_json, payments_file_name)

            menu_file_name = file_prefix + "/menu" + "/" + timestamp
            self.upload_blob(menu_json, menu_file_name)

            return users_file_name, payments_file_name, menu_file_name

        elif ds_type == "salesforce":
            dt_merchants = salesforce.get_multiple_rows(gen_dt_rows=gen_amount)
            merchants_json, ds_type = self.create_dataframe(dt_merchants, ds_type, gen_user_id=False, is_cnpj=gen_cnpj)

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            merchants_file_name = file_prefix + "/merchants" + "/" + timestamp
            self.upload_blob(merchants_json, merchants_file_name)

            return merchants_file_name,

        elif ds_type == "kafka":
            dt_merchants = merchants.get_multiple_rows(gen_dt_rows=gen_amount)
            merchants_json, ds_type = self.create_dataframe(dt_merchants, ds_type, gen_user_id=False, is_cnpj=gen_cnpj)

            dt_users = self.stored_data.get('users')
            dt_drivers = self.stored_data.get('drivers')
            dt_restaurants = self.stored_data.get('restaurants')
            dt_menu = self.stored_data.get('menu')
            dt_payments = self.stored_data.get('payments')

            user_cpfs = [user['cpf'] for user in dt_users]
            driver_license_numbers = [driver['license_number'] for driver in dt_drivers]
            restaurant_cnpjs = [restaurant['cnpj'] for restaurant in dt_restaurants]
            menu_ids = [menu_item['menu_id'] for menu_item in dt_menu]
            payment_ids = [payment['uuid'] for payment in dt_payments]

            events = orders.Orders.get_multiple_rows(
                gen_dt_rows=gen_amount,
                user_cpfs=user_cpfs,
                restaurant_cnpjs=restaurant_cnpjs,
                menu_ids=menu_ids,
                driver_license_numbers=driver_license_numbers,
                payment_ids=payment_ids
            )

            order_status = status.OrderStatus.get_multiple_order_statuses([event['order_id'] for event in events])

            orders_json, ds_type = self.create_dataframe(events, ds_type, gen_user_id=False)
            order_status_json, ds_type = self.create_dataframe(order_status, ds_type, gen_user_id=False)

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            merchants_file_name = file_prefix + "/merchants" + "/" + timestamp
            self.upload_blob(merchants_json, merchants_file_name)

            orders_file_name = file_prefix + "/orders" + "/" + timestamp
            self.upload_blob(orders_json, orders_file_name)

            orders_status_file_name = file_prefix + "/status" + "/" + timestamp
            self.upload_blob(order_status_json, orders_status_file_name)

            return orders_file_name, orders_status_file_name, merchants_file_name,


class MongoDBStorage:
    """
    This class is responsible for handling MongoDB operations, including inserting data into specific collections.
    """

    def __init__(self, uri, db_name):
        """
        Initialize MongoDB client and set database.

        Args:
            uri: MongoDB URI connection string.
            db_name: MongoDB database name.
        """
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def insert_data(self, collection_name, json_data):
        """
        Insert data into the specified MongoDB collection.

        Args:
            collection_name: The name of the MongoDB collection.
            json_data: The JSON data to insert.
        """
        collection = self.db[collection_name]
        data = pd.read_json(StringIO(json_data.decode('utf-8')))
        records = data.to_dict(orient='records')
        collection.insert_many(records)
        print(f"Inserted {len(records)} records into MongoDB collection: {collection_name}")

    def insert_users(self, json_data):
        """Inserts users data into MongoDB collection."""
        self.insert_data('users', json_data)

    def insert_customers(self, json_data):
        """Inserts customers data into MongoDB collection."""
        self.insert_data('customers', json_data)

    def insert_drivers(self, json_data):
        """Inserts drivers data into MongoDB collection."""
        self.insert_data('drivers', json_data)

    def insert_restaurants(self, json_data):
        """Inserts restaurants data into MongoDB collection."""
        self.insert_data('restaurants', json_data)

    def insert_menu(self, json_data):
        """Inserts menu data into MongoDB collection."""
        self.insert_data('menu', json_data)

    def insert_payments(self, json_data):
        """Inserts payments data into MongoDB collection."""
        self.insert_data('payments', json_data)

    def insert_orders(self, json_data):
        """Inserts orders data into MongoDB collection."""
        self.insert_data('orders', json_data)

    def insert_status(self, json_data):
        """Inserts order status data into MongoDB collection."""
        self.insert_data('status', json_data)