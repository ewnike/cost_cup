"""
August 11, 2024.

Code for inserting newly created
Corsi data into defined test tables in the
hockey_stats database.

Eric Winiecke.
"""

import os

import pandas as pd
from sqlalchemy import BigInteger, Column, Float, Integer, MetaData, Table
from sqlalchemy.orm import sessionmaker

from data_processing_utils import insert_data
from db_utils import get_db_engine, get_metadata

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# Define metadata and tables
metadata = MetaData()

# Define seasons
seasons = ["20152016", "20162017", "20172018"]


def create_corsi_table(table_name):
    """Define table creation function for test tables to avoid overwriting real data."""
    return Table(
        table_name,
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("team_id", Integer),
        Column("corsi_for", Float, nullable=True),
        Column("corsi_against", Float, nullable=True),
        Column("corsi", Float, nullable=True),
        Column("CF_Percent", Float, nullable=True),
    )


# Create test tables dynamically (with "_test" suffix)
tables = {season: create_corsi_table(f"raw_corsi_{season}") for season in seasons}
metadata.create_all(engine)  # Ensure test tables are created

# Initialize database session
Session = sessionmaker(bind=engine)

# Dynamically construct file paths (makes the code portable)
base_dir = os.getcwd()  # Get the current working directory
csv_dir = os.path.join(base_dir, "corsi_stats")  # Path to CSV directory

# Define CSV file paths and test table names dynamically
csv_files_and_mappings = [
    (os.path.join(csv_dir, f"corsi_stats_{season}.csv"), f"raw_corsi_{season}")
    for season in seasons
]

# Insert data into test tables
with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if os.path.exists(file_path):  # Ensure file exists
            df = pd.read_csv(file_path)  # Read CSV into DataFrame
            insert_data(engine, table_name, file_path)

        else:
            print(f"File not found: {file_path}, skipping...")


print("Data inserted successfully into all test tables.")
