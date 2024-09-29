"""
CLI that uses Typer to build the command line interface.

python cli.py --help

python cli.py all
python cli.py mssql
python cli.py postgres
python cli.py mongodb
python cli.py kafka
"""

import typer
import os

from rich import print
from main import Storage
from dotenv import load_dotenv

load_dotenv()

bucket_name = os.getenv("GCS_BUCKET_NAME")


def main(dstype: str):
    """
    Perform actions based on the specified data source type.

    Allowed types are: mssql, postgres, mongodb, kafka & all

    Args:
        dstype: The type of the data source.
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


if __name__ == "__main__":
    typer.run(main)
