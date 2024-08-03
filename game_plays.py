import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.orm import sessionmaker
from db_utils import clean_data, insert_data, inspect_data

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

# Define table schema for game_plays
metadata = MetaData()

game_plays = Table(
    "game_plays",
    metadata,
    Column("play_id", String(20), primary_key=True),
    Column("game_id", BigInteger),
    Column("team_id_for", Integer, nullable=True),
    Column("team_id_against", Integer, nullable=True),
    Column("event", String(50)),
    Column("secondaryType", String(50)),
    Column("x", Float, nullable=True),
    Column("y", Float, nullable=True),
    Column("period", Integer),
    Column("periodType", String(50)),
    Column("periodTime", Integer),
    Column("periodTimeRemaining", Integer),
    Column("dateTime", DateTime(timezone=False)),
    Column("goals_away", Integer, nullable=True),
    Column("goals_home", Integer, nullable=True),
    Column("description", String(255)),
    Column("st_x", Integer, nullable=True),
    Column("st_y", Integer, nullable=True),
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


# Define the column mapping for game_plays.csv
game_plays_column_mapping = {
    "play_id": "play_id",
    "game_id": "game_id",
    "team_id_for": "team_id_for",
    "team_id_against": "team_id_against",
    "event": "event",
    "secondaryType": "secondaryType",
    "x": "x",
    "y": "y",
    "period": "period",
    "periodType": "periodType",
    "periodTime": "periodTime",
    "periodTimeRemaining": "periodTimeRemaining",
    "dateTime": "dateTime",
    "goals_away": "goals_away",
    "goals_home": "goals_home",
    "description": "description",
    "st_x": "st_x",
    "st_y": "st_y",
}


# Main function to handle the workflow
def main():
    local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
    game_plays_csv_file_path = os.path.join(local_extract_path, "game_plays.csv")

    # Create the table if it does not exist
    create_table(engine)

    # Process and insert game_plays.csv
    if os.path.exists(game_plays_csv_file_path):
        process_and_insert_csv(
            game_plays_csv_file_path, game_plays, game_plays_column_mapping
        )
    else:
        print(f"CSV file {game_plays_csv_file_path} not found")


if __name__ == "__main__":
    main()
