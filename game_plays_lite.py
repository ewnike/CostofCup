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

query = "SELECT * FROM game_plays"
df = pd.read_sql_query(query, engine)

output_file = f"game_plays_lite_{num_rows}_rows.csv"

# Use .head() method to select the top num_rows rows
df.head(num_rows).to_csv(output_file, index=False)

print(f"Data has been written to {output_file}")



