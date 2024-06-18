import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from dotenv import load_dotenv
import psutil  # You may need to install this package

# Check if available memory is below a certain threshold (e.g., 100MB)
memory_threshold = 100 * 1024 * 1024  # 100 MB in bytes
available_memory = psutil.virtual_memory().available

memory_low = available_memory < memory_threshold

if memory_low:
    print('Memory is low!')

# Load environment variables from the .env file
load_dotenv()

# Retrieve database connection parameters from environment variables
DATABASE_TYPE = os.getenv('DATABASE_TYPE')
DBAPI = os.getenv('DBAPI')
ENDPOINT = os.getenv('ENDPOINT')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
PORT = int(os.getenv('PORT', 5433))  # Provide default value if not set
DATABASE = os.getenv('DATABASE')

# Create the connection string
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

engine = create_engine(connection_string)



directories = [
    #r"C:\Users\eric\Documents\cost_of_cup\Kaggle_stats",
    #r"C:\Users\eric\Documents\cost_of_cup\team_files",
    #r"C:\Users\eric\Documents\cost_of_cup\player_files"
    # Add more directories as needed
]


for directory_path in directories:
    print(f"Processing directory: {directory_path}")
    
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            # Load the CSV file into a pandas DataFrame
            file_path = os.path.join(directory_path, filename)
            table_name = os.path.splitext(filename)[0]
            
            try:
                if memory_low:
                    for chunk in pd.read_csv(file_path, chunksize=10000, index_col = None):
                        chunk.to_sql(table_name, engine, index=False, if_exists='replace')
               
                    else:
                        df = pd.read_csv(file_path, index_col=None)
                        df.to_sql(table_name, engine, index=False, if_exists='replace')
                        
                    print(f"Table '{table_name}' created successfully.")
                    
            except SQLAlchemyError as e:
                print(f"Error occurred while creating table '{table_name}': {e}")
                
            except Exception as e:
                print(f"Error occurred while processing file '{file_path}': {e}")
                

            
            

                
  
       
            

