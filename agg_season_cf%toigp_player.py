import pandas as pd
import os
import sys
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

        # Extract the season from the filename
        # Assuming filename format is "corsix_20152016.csv"
        season = filename.split('_')[1].split('.')[0]
        
        # Display the first few rows of the DataFrame
        print(f"Original DataFrame ({filename}):")
        print(df.head())
        
        agg_df = df.dropna(subset=['salary', 'CF_Percent'])
        grouped_agg_df = agg_df.groupby(['player_id', 'team_id']).agg({
            'first_name': 'first',
            'last_name': 'first',
            'cap_hit': 'first',
            'salary': 'first',
            'CF_Percent': 'mean',
            'toi': 'mean'
        }).reset_index()
        
        # Count the number of games played for each player_id per team
        df_counts_per_team = agg_df.groupby(['player_id', 'team_id']).size().reset_index(name='games_played')
        
        # Calculate the total number of games played for each player_id across all teams
        df_counts_total = df_counts_per_team.groupby('player_id')['games_played'].sum().reset_index(name='total_games_played')
        
        # Merge the per-team game counts with the grouped data
        df_agg_final = pd.merge(grouped_agg_df, df_counts_per_team, on=['player_id', 'team_id'])
        
        # Merge the total game counts with the final data
        df_agg_final = pd.merge(df_agg_final, df_counts_total, on='player_id')
        
        # Filter rows where 'total_games_played' > 56 (82 * 0.68)
        df_agg_filtered = df_agg_final[df_agg_final['total_games_played'] > 82 * 0.68]
        
        # Adjust CF% by multiplying by 100 and rounding to 4 decimal places
        df_agg_filtered['CF%_adjusted'] = (df_agg_filtered['CF_Percent'] * 100).round(4)
        
        # Sort the final DataFrame by 'total_games_played' in descending order and reset the index
        df_agg_sorted = df_agg_filtered.sort_values(by='total_games_played', ascending=False).reset_index(drop=True)

        # Display the first few rows of the sorted and reset DataFrame
        print(f"Sorted and Reset DataFrame ({filename} by total games played in descending order):")
        print(df_agg_sorted.head())
        
        # Save the sorted and reset DataFrame to a new CSV file and dir
        processed_filename = f"processed_{season}.csv"
        processed_file_path = os.path.join(processed_directory, processed_filename)
        df_agg_sorted.to_csv(processed_file_path, index=False)

        print(f"Processed DataFrame saved to {processed_file_path}")
        
        # Insert the processed data into a new table in the database
        table_name = f"season_{season}_processed"
        try:
            df_agg_sorted.to_sql(table_name, engine, index=False, if_exists='replace')
            print(f"Table '{table_name}' created successfully in the database.")
        except SQLAlchemyError as e:
            print(f"Error occurred while creating table '{table_name}': {e}")

            
            
