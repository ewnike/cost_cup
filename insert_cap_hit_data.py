"""
August 11, 2024
Code to insert data scraped from spotrac
website into data table in hockey_stats
database.
Eric Winiecke
"""

import os

import pandas as pd

from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()

# Define metadata and tables
metadata = MetaData()


def create_caphit_table(table_name):
    """Define table creation function to avoid repetition"""
    return Table(
        table_name,
        metadata,
        Column("firstName", String(50)),
        Column("lastName", String(50)),
        Column("capHit", String(50)),
    )


# Create tables for each season
seasons = ["20152016", "20162017", "20172018"]
tables = {season: create_caphit_table(f"player_cap_hit_{season}") for season in seasons}

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)


def insert_data_from_csv(engine, table_name, file_path):
    """insert data"""
    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
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
        "/Users/ericwiniecke/Documents/github/cost_cup/player_cap_hits/player_cap_hits_2015.csv",
        "player_cap_hit_20152016",
    ),
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/player_cap_hits/player_cap_hits_2016.csv",
        "player_cap_hit_20162017",
    ),
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/player_cap_hits/player_cap_hits_2017.csv",
        "player_cap_hit_20172018",
    ),
]

with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        insert_data_from_csv(engine, table_name, file_path)

    print("Data inserted successfully into all tables")
