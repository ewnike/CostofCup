import json
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Define database connection parameters
db_config = {
    "database": os.getenv('DB_NAME', 'my_database'),
    "user": os.getenv('DB_USER', 'my_user'),
    "password": os.getenv('DB_PASSPHRASE', 'my_password'),
    "host": os.getenv('DB_HOST', 'localhost'),
    "port": os.getenv('DB_PORT', 5433)
}

# Write the database configuration to a JSON file
with open('database.config.json', 'w') as config_file:
    json.dump(db_config, config_file, indent=4)

print("database.config.json file has been generated.")
