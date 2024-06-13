import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import datetime
import psycopg2
import sys
import getpass
import zipfile

# Read CSV file into DataFrame
df = pd.read_csv(r"kaggle_stats/team_info.csv")

# Database connection details
DATABASE_TYPE = "postgresql"
DBAPI = "psycopg2"
ENDPOINT = "localhost"
USER = "postgres"
PASSWORD = getpass.getpass(prompt="Password: ", stream=None)
PORT = 5433  # default port for PostgreSQL
DATABASE = "postgres"

# Create the connection string
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
engine = create_engine(connection_string)

# Insert data into PostgreSQL table
df.to_sql("team_info", engine, if_exists="replace", index=False)
