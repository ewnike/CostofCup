# This is a helper function to connect to the MADS_NHL database
# It uses load_dotenv() and takes advantage of the python .env so no private information is displayed when using.

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

files_to_update = ['corsix_20152016', 'corsix_20162017', 'corsix_20172018']


try:
    #Construct SQL UPDATE statement
    for file_name in files_to_update:
        table_name = file_name.replace(' ', '_').lower()
        

        update_query = text(f"""
            UPDATE {table_name} AS c
            SET
                first_name = p."firstName",
                last_name = p."lastName",
                primary_position = p."primaryPosition"
            FROM player_info AS p
            WHERE c.player_id = p.player_id
        """)
        
    
     #Execute Update Statement   
    session.execute(update_query)
    
    print(f"Updated {table_name} successfully.")
    
    #Commit transactions
    session.commit()
    

except SQLAlchemyError as e:
    print(f"Error occurred: {e}")
    session.rollback()  # Rollback the transaction on error
    
finally:
    # Close cursor and connection
    session.close()

    
    
                
            