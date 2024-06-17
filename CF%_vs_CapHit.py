import pandas as pd
import os
import altair as alt

# Define the processed directory containing the CSV files
processed_directory = r"C:\Users\eric\Documents\cost_of_cup\processed_files"

# Define the directory to save the charts
charts_directory = r"C:\Users\eric\Documents\cost_of_cup\charts"

# Create the charts directory if it doesn't exist
os.makedirs(charts_directory, exist_ok=True)

# DataFrames to collect all seasons data
all_seasons_df = []

# Iterate over all CSV files in the processed directory
for filename in os.listdir(processed_directory):
    if filename.endswith('.csv'):
        # Load the CSV file into a pandas DataFrame
        file_path = os.path.join(processed_directory, filename)
        df = pd.read_csv(file_path)
        
        # Extract the season from the filename
        season = filename.split('.')[0]  # Assuming the filename format is "season.csv"
        df['season'] = season
        
        # Append to the all seasons DataFrame list
        all_seasons_df.append(df)

# Create charts using Altair for each season
for season_df in all_seasons_df:
    season = season_df['season'].iloc[0]  # Get the season name
    chart = alt.Chart(season_df).mark_circle(size=60).encode(
        x=alt.X('cap_hit:Q', title='Cap Hit'),
        y=alt.Y('CF%_adjusted:Q', title='CF% (Adjusted)'),
        color=alt.Color('team_id:N', title='Team ID'),
        tooltip=['player_id', 'team_id', 'cap_hit', 'CF%_adjusted', 'season']
    ).properties(
        title=f'CF% vs Cap Hit for {season}'
    ).interactive()

    chart.display()
    # Save the chart as an HTML file in the charts directory
    chart.save(os.path.join(charts_directory, f'CF%_vs_CapHit_{season}.html'))

print("Charts created and saved.")

