import os

import pandas as pd
from db_utils import clean_data, insert_data
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5432))
DATABASE = os.getenv("DATABASE", "hockey_stats")

# Create the connection string
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)

# Define table schemas
metadata = MetaData()

game = Table(
    "game",
    metadata,
    Column("game_id", BigInteger, primary_key=True),
    Column("season", Integer),
    Column("type", String(50)),
    Column("date_time_GMT", DateTime(timezone=True)),
    Column("away_team_id", Integer),
    Column("home_team_id", Integer),
    Column("away_goals", Integer),
    Column("home_goals", Integer),
    Column("outcome", String(100)),
    Column("home_rink_side_start", String(100)),
    Column("venue", String(100)),
    Column("venue_link", String(100)),
    Column("venue_time_zone_id", String(50)),
    Column("venue_time_zone_offset", Integer),
    Column("venue_time_zone_tz", String(25)),
)

# Create the database engine
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


# Function to create the table if it does not exist
def create_table(engine):
    metadata.create_all(engine)


# Function to process and insert data from a CSV file
def process_and_insert_csv(csv_file_path, table, column_mapping):
    try:
        print(f"Processing {csv_file_path} for table {table.name}")
        df = pd.read_csv(csv_file_path)
        print(f"DataFrame for {table.name} loaded with {len(df)} records")

        # Print the datatype of each column
        print("Column Datatypes:")
        print(df.dtypes)

        # Print a sample of the data to debug
        print("Sample data:")
        print(df.head())

        # Clean the data
        df = clean_data(df, column_mapping)
        print(f"DataFrame for {table.name} cleaned with {len(df)} records")

        # Print a sample of the cleaned data to debug
        print("Cleaned sample data:")
        print(df.head())

        # Insert the cleaned data into the database
        insert_data(df, table, Session())

    except FileNotFoundError as e:
        print(f"File not found: {csv_file_path} - {e}")


# Define the column mapping for game.csv
column_mapping = {
    "game_id": "game_id",
    "season": "season",
    "type": "type",
    "date_time_GMT": "date_time_GMT",
    "away_team_id": "away_team_id",
    "home_team_id": "home_team_id",
    "away_goals": "away_goals",
    "home_goals": "home_goals",
    "outcome": "outcome",
    "home_rink_side_start": "home_rink_side_start",
    "venue": "venue",
    "venue_link": "venue_link",
    "venue_time_zone_id": "venue_time_zone_id",
    "venue_time_zone_offset": "venue_time_zone_offset",
    "venue_time_zone_tz": "venue_time_zone_tz",
}


# Main function to handle the workflow
def main():
    local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
    game_csv_file_path = os.path.join(local_extract_path, "game.csv")

    # Create the table if it does not exist
    create_table(engine)

    if os.path.exists(game_csv_file_path):
        process_and_insert_csv(game_csv_file_path, game, column_mapping)
    else:
        print(f"CSV file {game_csv_file_path} not found")


if __name__ == "__main__":
    main()
