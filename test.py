import pandas as pd
import zipfile
import os
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import sys
import getpass

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

directory_path = r"C:\Users\eric\Documents\cost_of_cup"
# List all files in the directory
for file_name in os.listdir(directory_path):
    if file_name.endswith(".zip"):
        zip_file_path = os.path.join(directory_path, file_name)
        print(f"Processing zip file: {zip_file_path}")
        with zipfile.ZipFile(zip_file_path, "r") as z:
            # List all files in the zip archive
            for contained_file in z.namelist():
                if contained_file.endswith(".csv"):
                    print(f"Reading CSV file: {contained_file}")
                    with z.open(contained_file) as csvfile:
                        df = pd.read_csv(csvfile)
                        # Insert the data into the PostgreSQL table
                        table_name = os.path.splitext(contained_file)[0]
                        df.to_sql(table_name, engine, if_exists="replace", index=False)
                        print(f"Inserted data from {contained_file} into {table_name}")
