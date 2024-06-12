import pandas as pd
from pprint import pprint

# Read CSV file into DataFrame
Corsi_Lite_df = pd.read_csv("Corsi_Lite.csv")
 
# Filter rows where timeOnIce is greater than 1000
filtered_df = Corsi_Lite_df.loc[Corsi_Lite_df['C'] > 0]

# Print DataFrame with pandas display options
pd.set_option('display.max_rows', 10)  # Adjust as needed
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)  # Set display width
print(filtered_df)

# Convert the filtered DataFrame to a dictionary for pretty-printing
#filtered_dict = filtered_df.to_dict(orient='records')

# Pretty print the first few records of the dictionary to avoid long output
#pprint(filtered_dict[:10])  # Adjust the number of items as needed

    