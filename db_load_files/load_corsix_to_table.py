  
import pandas as pd
import os
from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, String, MetaData, Float
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import psutil  # You may need to install this package

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

# Define metadata and tables
metadata = MetaData()

corsixx_20152016 = Table(
    'corsixx_20152016', metadata,
    Column('game_id', BigInteger),
    Column('player_id', BigInteger),
    Column('team_id', Integer),
    Column('CF', Float, nullable=True),
    Column('CA', Float, nullable=True),
    Column('C', Float, nullable=True),
    Column('CF_Percent', Float, nullable=True),
)

corsixx_20162017 = Table(
    'corsixx_20162017', metadata,
    Column('game_id', BigInteger),
    Column('player_id', BigInteger),
    Column('team_id', Integer),
    Column('CF', Float, nullable=True),
    Column('CA', Float, nullable=True),
    Column('C', Float, nullable=True),
    Column('CF_Percent', Float, nullable=True),
)

corsixx_20172018 = Table(
    'corsixx_20172018', metadata,
    Column('game_id', BigInteger),
    Column('player_id', BigInteger),
    Column('team_id', Integer),
    Column('CF', Float, nullable=True),
    Column('CA', Float, nullable=True),
    Column('C', Float, nullable=True),
    Column('CF_Percent', Float, nullable=True),
)

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def insert_data_from_csv(session, table, file_path, column_mapping):
    try:
        df = pd.read_csv(file_path)
        for index, row in df.iterrows():
            data = {column: row[csv_column] for column, csv_column in column_mapping.items()}
            session.execute(table.insert().values(**data))
        session.commit()
        print(f'Data inserted successfully into {table.name}')
    except SQLAlchemyError as e:
        session.rollback()
        print(f'Error inserting data into {table.name}: {e}')
    except FileNotFoundError as e:
        print(f"File not found: {file_path} - {e}")
    except Exception as e:
        print(f"Error occurred while processing file '{file_path}': {e}")

# Define directories and mappings
csv_files_and_mappings = [
    (r"C:\Users\eric\Documents\cost_of_cup\corsi_vals_II\corsixx_20152016.csv", corsixx_20152016, {'game_id': 'game_id', 'player_id': 'player_id', 'team_id': 'team_id', 'CF': 'CF', 'CA': 'CA', 'C': 'C', 'CF_Percent': 'CF_Percent'}),
    (r"C:\Users\eric\Documents\cost_of_cup\corsi_vals_II\corsixx_20162017.csv", corsixx_20162017, {'game_id': 'game_id', 'player_id': 'player_id', 'team_id': 'team_id', 'CF': 'CF', 'CA': 'CA', 'C': 'C', 'CF_Percent': 'CF_Percent'}),
    (r"C:\Users\eric\Documents\cost_of_cup\corsi_vals_II\corsixx_20172018.csv", corsixx_20172018, {'game_id': 'game_id', 'player_id': 'player_id', 'team_id': 'team_id', 'CF': 'CF', 'CA': 'CA', 'C': 'C', 'CF_Percent': 'CF_Percent'}),
]

with Session() as session:
    for file_path, table, column_mapping in csv_files_and_mappings:
        insert_data_from_csv(session, table, file_path, column_mapping)

    print('Data inserted successfully into all tables')
