import pandas as pd
import psycopg2
from sqlalchemy import create_engine
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

# Get the number of rows from the user
num_rows = int(input("Enter the number of rows you want in the file: "))
tables = ["game_plays", "game_shifts", "game_skater_stats"]

# Loop through each table to read data and create lite files
for table in tables:
    # Define the query to read data from the current table
    query = f"SELECT * FROM {table} LIMIT {num_rows}"
    
    # Read the data from the current table
    df = pd.read_sql_query(query, engine)
    
    # Define the output file name
    output_file = f"{table}_lite.csv"
    
    # Select the top num_rows_per_file rows
    df.head(num_rows).to_csv(output_file, index=False)
    
    print(f"Data has been written to {output_file}")

print("All files have been created.")