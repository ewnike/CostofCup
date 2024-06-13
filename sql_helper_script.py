#This is a helper function to connect to the MADS_NHL database
#It uses load_dotenv() and takes advantage of the python .env so no private information is displayed when using.

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

#Enter your query here:
    
query = 'SELECT * FROM "Corsi_20152016" LIMIT 5;'

try:
    # Read the SQL query into a DataFrame
    df = pd.read_sql(query, engine)
    
    # Print the DataFrame
    print(df)
    
    # Save the DataFrame to a CSV file without the index
    df.to_csv('Corsi_Test.csv', index=False)

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    engine.dispose()  # Close the engine
    
