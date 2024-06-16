import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT"))
DATABASE = os.getenv("DATABASE")

# Create the connection string
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
engine = create_engine(connection_string)


csv_dir = r"C:\Users\eric\Documents\cost_of_cup\corsi_vals_II"
processed_directory = r"C:\Users\eric\Documents\cost_of_cup\processed_files"

# Create the processed files directory if it doesn't exist
os.makedirs(processed_directory, exist_ok=True)



# Iterate over all CSV files in the directory
for filename in os.listdir(csv_dir):
    if filename.endswith('.csv'):
        # Load the CSV file into a pandas DataFrame
        file_path = os.path.join(csv_dir, filename)
        df = pd.read_csv(file_path)

        # Display the first few rows of the DataFrame
        print(f"Original DataFrame ({filename}):")
        print(df.head())
        
        agg_df = df.dropna(subset=['salary', 'CF_Percent'])
        grouped_agg_df = agg_df.groupby('player_id').agg({'team_id': 'first', 'first_name' : 'first' , 'last_name': 'first', 'cap_hit': 'first', 'salary': 'first','CF_Percent': 'mean', 'toi': 'mean'}).reset_index()
        
        # Count the number of games played for each player_id
        df_counts = agg_df.groupby('player_id').size().reset_index(name='games_played')

        # Merge the aggregated data with the counts
        df_agg_final = pd.merge(grouped_agg_df, df_counts, on='player_id')

        # Filter rows where 'games_played' > 56 (82 * 0.68) keeping players that have only played in 68% of the reg season games.
        df_agg_final = df_agg_final[df_agg_final['games_played'] > 82 * 0.68]

        # Sort the final DataFrame by 'games_played' in descending order
        df_agg_sorted = df_agg_final.sort_values(by='games_played', ascending=False).reset_index(drop=True)
        print(df_agg_sorted.head())
        print(df_agg_sorted.shape)
        
        # Save the sorted and reset DataFrame to a new CSV file and dir
        processed_filename = f"processed_{filename}"
        processed_file_path = os.path.join(processed_directory, processed_filename)
        df_agg_sorted.to_csv(processed_file_path, index=False)

        print(f"Processed DataFrame saved to {processed_file_path}")
        
        # Insert the processed data into a new table in the database
        table_name = f"{os.path.splitext(filename)[0]}_processed"
        try:
            df_agg_sorted.to_sql(table_name, engine, index=False, if_exists='replace')
            print(f"Table '{table_name}' created successfully in the database.")
        except SQLAlchemyError as e:
            print(f"Error occurred while creating table '{table_name}': {e}")