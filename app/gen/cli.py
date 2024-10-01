"""
python cli.py all
python cli.py atlas
"""

import typer
import os
from rich import print
from main import Storage
from dotenv import load_dotenv
from main import MongoDBStorage
from src.objects.users import Users
from src.objects.payments import Payments
from src.api import api_requests

load_dotenv()

bucket_name = os.getenv("GCS_BUCKET_NAME")
mongo_uri = os.getenv("MONGODB_URI")
db_name = os.getenv("MONGODB_DB_NAME")

gen_amount = 500
api = api_requests.Requests()


def handle_atlas_storage():
    """
    Handle MongoDB Atlas-specific storage logic for multiple datasets.
    """
    mongo_storage_instance = MongoDBStorage(mongo_uri, db_name)

    cpf_list = [api.gen_cpf() for _ in range(gen_amount)]

    dt_users = Users.get_multiple_rows(gen_dt_rows=gen_amount)
    users_json, _ = Storage.create_dataframe(dt_users, "users", is_cpf=True, gen_user_id=True, cpf_list=cpf_list)
    mongo_storage_instance.insert_users(users_json)

    dt_payments = Payments.get_multiple_rows(gen_dt_rows=gen_amount)
    payments_json, _ = Storage.create_dataframe(dt_payments, "payments", is_cpf=True, gen_user_id=True, cpf_list=cpf_list)
    mongo_storage_instance.insert_payments(payments_json)


def handle_general_storage(dstype: str):
    """
    Handle the general storage logic for MSSQL, PostgreSQL, Salesforce, Kafka, and MongoDB.
    """
    storage_instance = Storage(bucket_name)

    if dstype == "mssql":
        print(storage_instance.write_file(ds_type="mssql"))
    elif dstype == "postgres":
        print(storage_instance.write_file(ds_type="postgres"))
    elif dstype == "mongodb":
        print(storage_instance.write_file(ds_type="mongodb"))
    elif dstype == "salesforce":
        print(storage_instance.write_file(ds_type="salesforce"))
    elif dstype == "kafka":
        print(storage_instance.write_file(ds_type="kafka"))
    elif dstype == "all":
        print(storage_instance.write_file(ds_type="mssql"))
        print(storage_instance.write_file(ds_type="postgres"))
        print(storage_instance.write_file(ds_type="mongodb"))
        print(storage_instance.write_file(ds_type="salesforce"))
        print(storage_instance.write_file(ds_type="kafka"))


def main(dstype: str):
    """
    Perform actions based on the specified data source type.

    Allowed types are: mssql, postgres, mongodb, kafka, atlas, & all

    Args:
        dstype: The type of the data source.
    """
    if dstype == "atlas":
        handle_atlas_storage()
    else:
        handle_general_storage(dstype)


if __name__ == "__main__":
    typer.run(main)
