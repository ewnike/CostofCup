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
DATABASE_TYPE = os.getenv('DATABASE_TYPE')
DBAPI = os.getenv('DBAPI')
ENDPOINT = os.getenv('ENDPOINT')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
PORT = int(os.getenv('PORT'))  # Provide default value if not set
print(f"Using port: {PORT}")
DATABASE = os.getenv('DATABASE')

connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

#print(connection_string)
engine = create_engine(connection_string)

Session = sessionmaker(bind=engine)

# Create a session
session = Session()


directory_path = r"C:\Users\eric\Documents\cost_of_cup\Kaggle_stats"

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
            session.rollback() #rollback the transaction error
        finally:
            session.close()
            
