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

files_to_update ' ['CorsiX_20152016', 'CorsiX_20172018', 'CorsiX_20182019']

cur=conn.cursor()

try:
    #Construct SQL UPDATE statement
    for file_name in files_to_update:
        table_name = file_name.replace(' ', '_').lower()
        
        update_query = sql.SQL("""
            UPDATE {} AS c
            SET
                first_name = p.first_name,
                last_name = p.last_name,
                primary_position = p.primary_position
            FROM player_info AS p
            WHERE c.player_id = p.player_id
        """).format(sql.Identifier(table_name))
        
    
     #Execute Update Statement   
    cur.execute(update_query)
    
    print(f"Updated {table_name} successfully.")
    
    #Commit transactions
    conn.commit()
    
except psycopg2.Error as e:
    print(f"Error updating data: {e}")
    conn.rollback()
    
finally:
    # Close cursor and connection
    cur.close()
    conn.close()
    
    
                
            