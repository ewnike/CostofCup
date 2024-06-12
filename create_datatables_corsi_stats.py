import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import getpass

# Database connection details
DATABASE_TYPE = "postgresql"
DBAPI = "psycopg2"
ENDPOINT = "localhost"
USER = "postgres"
PASSWORD = getpass.getpass(prompt="Password: ", stream=None)
PORT = 5433  # default port for PostgreSQL
DATABASE = "MADS_NHL"

# Create the connection string
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

engine = create_engine(connection_string)

directory_path = r"C:\Users\eric\Documents\cost_of_cup\corsi_vals"

for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):
        # Load the CSV file into a pandas DataFrame
        file_path = os.path.join(directory_path, filename)
        df = pd.read_csv(file_path)

        # Define the table name (without the .csv extension)
        table_name = os.path.splitext(filename)[0]
        # Write the DataFrame to the SQL database
        try:
            df.to_sql(table_name, engine, index=False, if_exists='replace')
            print(f"Table '{table_name}' created successfully.")
        except SQLAlchemyError as e:
            print(f"Error occurred while creating table '{table_name}': {e}")

