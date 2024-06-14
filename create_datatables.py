import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Database connection details
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT"))  # Provide default value if not set
print(f"Using port: {PORT}")
DATABASE = os.getenv("DATABASE")

connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

# print(connection_string)
engine = create_engine(connection_string)

Session = sessionmaker(bind=engine)

# Create a session
session = Session()

directory_path = (r"C:\Users\eric\Documents\cost_of_cup\corsi_vals_II")
# List all files in the directory
for file_name in os.listdir(directory_path):
    print(file_name)
    if file_name.endswith(".csv"):
        print(f"Reading CSV file: {file_name}")
        file_path = os.path.join(directory_path, file_name)
        with open(file_name) as csvfile:
            df = pd.read_csv(csvfile)
            # Insert the data into the PostgreSQL table
            table_name = os.path.splitext(file_name)[0]

            try:
                with engine.begin() as connection:
                    df.to_sql(table_name, connection, if_exists="replace", index=False)
                    print(f"Inserted data from {contained_file} into {table_name}")
            except SQLAlchemyError as e:
                    print(
                    f"Error inserting data from {contained_file} into {table_name}: {e}"
                    )
            # If there is an error, the transaction will be rolled back automatically

