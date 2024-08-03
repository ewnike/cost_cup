import os

import pandas as pd
from db_utils import clean_data, insert_data, inspect_data
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, create_engine
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

# Define table schema for game_skater_stats
metadata = MetaData()

game_skater_stats = Table(
    "game_skater_stats",
    metadata,
    Column("game_id", BigInteger),
    Column("player_id", BigInteger),
    Column("team_id", Integer),
    Column("timeOnIce", Integer),
    Column("assists", Integer),
    Column("goals", Integer),
    Column("shots", Integer),
    Column("hits", Integer),
    Column("powerPlayGoals", Integer),
    Column("powerPlayAssists", Integer),
    Column("penaltyMinutes", Integer),
    Column("faceOffWins", Integer),
    Column("faceoffTaken", Integer),
    Column("takeaways", Integer),
    Column("giveaways", Integer),
    Column("shortHandedGoals", Integer),
    Column("shortHandedAssists", Integer),
    Column("blocked", Integer),
    Column("plusMinus", Integer),
    Column("evenTimeOnIce", Integer),
    Column("shortHandedTimeOnIce", Integer),
    Column("powerPlayTimeOnIce", Integer),
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

        # Inspect data for potential errors
        inspect_data(df, column_mapping)

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


# Define the column mapping for game_skater_stats.csv
game_skater_stats_column_mapping = {
    "game_id": "game_id",
    "player_id": "player_id",
    "team_id": "team_id",
    "timeOnIce": "timeOnIce",
    "assists": "assists",
    "goals": "goals",
    "shots": "shots",
    "hits": "hits",
    "powerPlayGoals": "powerPlayGoals",
    "powerPlayAssists": "powerPlayAssists",
    "penaltyMinutes": "penaltyMinutes",
    "faceOffWins": "faceOffWins",
    "faceoffTaken": "faceoffTaken",
    "takeaways": "takeaways",
    "giveaways": "giveaways",
    "shortHandedGoals": "shortHandedGoals",
    "shortHandedAssists": "shortHandedAssists",
    "blocked": "blocked",
    "plusMinus": "plusMinus",
    "evenTimeOnIce": "evenTimeOnIce",
    "shortHandedTimeOnIce": "shortHandedTimeOnIce",
    "powerPlayTimeOnIce": "powerPlayTimeOnIce",
}


# Main function to handle the workflow
def main():
    local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
    game_skater_stats_csv_file_path = os.path.join(
        local_extract_path, "game_skater_stats.csv"
    )

    # Create the table if it does not exist
    create_table(engine)

    # Process and insert game_skater_stats.csv
    if os.path.exists(game_skater_stats_csv_file_path):
        process_and_insert_csv(
            game_skater_stats_csv_file_path,
            game_skater_stats,
            game_skater_stats_column_mapping,
        )
    else:
        print(f"CSV file {game_skater_stats_csv_file_path} not found")


if __name__ == "__main__":
    main()
