"""
January 5, 2025
Code to create datatables and insert
data into the team_event_total_games
tables for each season.
"""
import os

import pandas as pd

# from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    Table,
)

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()

metadata = MetaData()

def create_team_event_total_games_table(table_name):
    """Define table creation function to avoid repetition"""
    return Table(
        table_name,
        metadata,
        Column("team_id", Integer),
        Column("total_goals", Integer),
        Column("total_shots", Integer),
        Column("total_missed_shots", Integer),
        Column("total_blocked_shots_for", Integer),
        Column("total_goals_against", Integer),
        Column("total_shots_against", Integer),
        Column("total_missed_shots_against", Integer),
        Column("total_blocked_shots_against", Integer),
        Column("game_id", BigInteger),
    )


# Create tables for each season
seasons = ["20152016", "20162017", "20172018"]
tables = {season: create_team_event_total_games_table(f"team_event_totals_games_{season}") for season in seasons}

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)



def insert_data_from_csv(engine, table_name, file_path):
    """insert data"""
    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Data inserted successfully into {table_name}")

        # Remove the file after successful insertion
        os.remove(file_path)
        print(f"File {file_path} deleted successfully.")

    except SQLAlchemyError as e:
        print(f"Error inserting data into {table_name}: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {file_path} - {e}")
    except Exception as e:
        print(f"Error occurred while processing file '{file_path}': {e}")


# Define directories and mappings
csv_files_and_mappings = [
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/team_event_totals/team_event_totals_games_20152016.csv",
        "team_event_totals_games_20152016",
    ),
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/team_event_totals/team_event_totals_games_20162017.csv",
        "team_event_totals_games_20162017",
    ),
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/team_event_totals/team_event_totals_games_20172018.csv",
        "team_event_totals_games_20172018",
    ),
]


for file_path, table_name in csv_files_and_mappings:
    insert_data_from_csv(engine, table_name, file_path)

print("Data inserted successfully into all tables")