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

# Directory containing the CSV files
csv_directory = r"C:\Users\eric\Documents\cost_of_cup\corsi_vals_II"

try:
    # Add new columns to each table
    for file_name in files_to_update:
        table_name = file_name.replace(' ', '_').lower()
        add_columns_query = text(f"""
            ALTER TABLE "{table_name}"
            ADD COLUMN IF NOT EXISTS toi BIGINT;
        """)
        
        try:
            session.execute(add_columns_query)
            session.commit()  # Commit column additions immediately
            print(f"Added new column 'toi' to {file_name} successfully.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error adding column to {file_name}: {e}")

    # Construct and execute SQL UPDATE statement for each table
    for file_name in files_to_update:
        table_name = file_name.replace(' ', '_').lower()
        update_query = text(f"""
            UPDATE "{table_name}" AS c
            SET
                toi = gss."timeOnIce"
            FROM game_skater_stats AS gss
            WHERE c.player_id = gss.player_id
            AND c.game_id = gss.game_id
        """)
        
        try:
            session.execute(update_query)
            session.commit()  # Commit updates immediately
            print(f"Updated 'toi' column in {file_name} successfully.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error updating {file_name}: {e}")
            
    # If all database transactions are successful, update the CSV files
    for file_name in files_to_update:
        table_name = file_name.replace(' ', '_').lower()
        csv_file_path = os.path.join(csv_directory, f"{file_name}.csv")
        df = pd.read_sql_table(table_name, engine)

        # Write the DataFrame back to the CSV file
        df.to_csv(csv_file_path, index=False)
        print(f"Updated CSV file: {csv_file_path}")

except SQLAlchemyError as e:
    print(f"General error occurred: {e}")
    session.rollback()  # Rollback any remaining transaction on general error

finally:
    session.close()  # Ensure session is closed

    
    

    