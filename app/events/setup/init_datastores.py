# import libraries
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# get env
load_dotenv()

# load variables
# get from env
postgres = os.getenv("POSTGRES")
mysql = os.getenv("MYSQL")
mssql = os.getenv("MSSQL")

# building engines for relational databases
# postgres, mysql and mssql
postgres_engine = create_engine(postgres)
mysql_engine = create_engine(mysql)
mssql_engine = create_engine(mssql)