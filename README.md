# PostgreSQL  for Cost of Cup Project


## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Database Setup](#database-setup)
4. [Data Dump](#data-dump)
5. [File Manipulation](#file-manipulation)
6. [Using pgAdmin](#using-pgadmin)
7. [Running SQL Scripts](#running-sql-scripts)
8. [Python Setup](#python-setup)
9. [Uploading Data from a Data Dump](#uploading-data-from-a-data-dump)
10. [Troubleshooting](#troubleshooting)
11. [Additional Resources](#additional-resources)

## Introduction

This project involves setting up a PostgreSQL database, performing data dumps, and manipulating data files using pgAdmin, PostgreSQL, and Python.

## Prerequisites

Before starting, ensure you have the following installed:

- PostgreSQL
- pgAdmin
- Python 3.x
- pip (Python package installer)

## Database Setup

1. **Install PostgreSQL and pgAdmin**:
    - Follow the instructions on the [PostgreSQL](https://www.postgresql.org/download/) and [pgAdmin](https://www.pgadmin.org/download/) websites to download and install the software.

2. **Start PostgreSQL Server**:
    - Ensure your PostgreSQL server is running. You can start it from your terminal or command prompt:
    ```sh
    pg_ctl -D /usr/local/var/postgres start
    ```

3. **Create a New Database**:
    - Open pgAdmin and connect to your PostgreSQL server.
    - Right-click on `Databases` and select `Create > Database...`.
    - Enter the database name (e.g., `my_database`) and click `Save`.

4. **Create Tables**:
    - Open the Query Tool in pgAdmin.
    - Run the following SQL commands to create the necessary tables:
    ```sql
    CREATE TABLE player_20151016 (
        player_id INTEGER PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        cap_hit VARCHAR(255),
        salary VARCHAR(255)
    );

    CREATE TABLE Corsi_20152016 (
        id SERIAL PRIMARY KEY,
        player_id INTEGER,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        corsi_value NUMERIC,
        position VARCHAR(255)
    );
    ```

## Data Dump

1. **Export Data**:
    - To export data from a table to a file, you can use the `COPY` command in pgAdmin or psql:
    ```sql
    COPY player_20151016 TO '/path/to/player_20151016.csv' CSV HEADER;
    ```

2. **Import Data**:
    - To import data from a CSV file into a table:
    ```sql
    COPY player_20151016 FROM '/path/to/player_20151016.csv' CSV HEADER;
    ```

## File Manipulation

1. **Add New Columns**:
    - To add new columns to a table:
    ```sql
    ALTER TABLE Corsi_20152016
    ADD COLUMN cap_hit VARCHAR(255),
    ADD COLUMN salary_hit VARCHAR(255);
    ```

2. **Update Data**:
    - To update the table with data from another table based on matching columns:
    ```sql
    UPDATE Corsi_20152016 AS c
    SET cap_hit = p.cap_hit,
        salary_hit = p.salary
    FROM player_20151016 AS p
    WHERE c.first_name = p.first_name
      AND c.last_name = p.last_name;
    ```

## Using pgAdmin

1. **Connecting to Server**:
    - Open pgAdmin and create a new server connection.
    - Provide connection details (name, host, port, username, password).

2. **Running Queries**:
    - Open the Query Tool.
    - Write and execute SQL queries.

3. **Managing Tables**:
    - Right-click on a table to perform operations such as viewing data, truncating, dropping, etc.

## Running SQL Scripts

1. **Open Query Tool**:
    - In pgAdmin, open the Query Tool to write and execute SQL scripts.

2. **Execute Scripts**:
    - Copy your SQL script into the Query Tool and click the execute button (lightning bolt icon).

3. **Save and Load Scripts**:
    - You can save your scripts for future use by clicking the save icon.
    - To load a saved script, click the open icon and select your script file.

## Python Setup

1. **Install Python Libraries**:
    - Install the required Python libraries using `pip`:
    ```sh
    pip install psycopg2-binary sqlalchemy python-dotenv pandas
    ```

2. **Create a `.env` File**:
    - Store your database configuration details in a `.env` file:
    ```plaintext
    DATABASE_URL=postgresql://username:password@localhost/my_database
    ```

3. **Database Configuration and Connection**:
    - Use the following Python script to connect to the database and perform operations:
    ```python
    import os
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    import pandas as pd

    # Load environment variables from .env file
    load_dotenv()

    # Get the database URL from the environment variable
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        raise ValueError("No DATABASE_URL found in environment variables")

    # Create an SQLAlchemy engine
    engine = create_engine(DATABASE_URL)

    # Example query to verify connection and fetch data
    query = "SELECT * FROM player_20151016"
    df = pd.read_sql(query, engine)
    print(df)
    ```

## Uploading Data from a Data Dump

This section provides instructions on how to upload data from a data dump into your PostgreSQL database using pgAdmin and psql.

### Prerequisites

- Ensure you have downloaded the data dump file from the provided link.
- The data dump file is typically in `.sql` or `.csv` format.

### Download the Data Dump

1. **Download the Data Dump File**: Click on the provided link to download the data dump file.
    - Example link: [Download Data Dump](http://example.com/data-dump.sql)
    - Save the file to a known location on your computer.

### Using pgAdmin to Upload Data

#### For `.sql` Files:

1. **Open pgAdmin**: Launch pgAdmin and connect to your PostgreSQL server.
2. **Select Database**: Right-click on the database where you want to upload the data and select `Query Tool`.
3. **Load the SQL File**:
    - In the Query Tool, click on the open file icon or press `Ctrl + O`.
    - Browse to the location where you saved the data dump file and select it.
4. **Execute the SQL File**:
    - Click the execute button (lightning bolt icon) or press `F5` to run the SQL script and upload the data.

#### For `.csv` Files:

1. **Open pgAdmin**: Launch pgAdmin and connect to your PostgreSQL server.
2. **Select Database**: Right-click on the database where you want to upload the data and select `Query Tool`.
3. **Create Table**: Ensure the table structure matches the CSV file format. If the table does not exist, create it using the following SQL command (modify as needed):
    ```sql
    CREATE TABLE player_20151016 (
        player_id INTEGER PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        cap_hit VARCHAR(255),
        salary VARCHAR(255)
    );
    ```
4. **Import CSV Data**:
    - Use the `COPY` command to import data from the CSV file:
    ```sql
    COPY player_20151016 FROM '/path/to/player_20151016.csv' CSV HEADER;
    ```
    - Make sure to replace `/path/to/player_20151016.csv` with the actual file path.

### Using psql to Upload Data

#### For `.sql` Files:

1. **Open Terminal**: Open a terminal or command prompt.
2. **Navigate to File Location**: Navigate to the directory where you saved the data dump file.
3. **Run psql Command**: Use the `psql` command to upload the data:
    ```sh
    psql -U username -d database_name -f data-dump.sql
    ```
    - Replace `username` with your PostgreSQL username.
    - Replace `database_name` with the name of your database.
    - Replace `data-dump.sql` with the name of your data dump file.

#### For `.csv` Files:

1. **Open Terminal**: Open a terminal or command prompt.
2. **Navigate to File Location**: Navigate to the directory where you saved the CSV file.
3. **Run psql Command**: Use the `psql` command to import data:
    ```sh
    psql -U username -d database_name -c "\COPY player_20151016 FROM '/path/to/player_20151016.csv' CSV HEADER;"
    ```
    - Replace `username` with your PostgreSQL username.
    - Replace `database_name` with the name of your database.
    - Replace `/path/to/player_20151016.csv` with the actual file path.

### Verifying the Data Upload

1. **Open pgAdmin**: Launch pgAdmin and connect to your PostgreSQL server.
2. **Check the Data**: Right-click on the table and select `View/Edit Data > All Rows` to verify that the data has been uploaded correctly.

By following these instructions, you can successfully upload data from the provided data dump file into your PostgreSQL database using either pgAdmin or psql.

## Troubleshooting

1. **Connection Issues**:
    - Ensure PostgreSQL server is running.
    - Verify connection details in pgAdmin.

2. **Permission Issues**:
    - Ensure you have the necessary permissions to perform operations on the database.

3. **Syntax Errors**:
    - Double-check SQL syntax and ensure you’re using the correct commands.

## Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [pgAdmin Documentation](https://www.pgadmin.org/docs/)
- [SQL Tutorial](https://www.w3schools.com/sql/)

By following these instructions, you should be able to set up your PostgreSQL database, manage data dumps, and manipulate data files effectively using pgAdmin and Python. If you encounter any issues, refer to the troubleshooting section or consult the additional resources provided.
