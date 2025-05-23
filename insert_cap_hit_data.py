"""
August 11, 2024.

Code to insert data scraped from Spotrac
website into the hockey_stats database.

Eric Winiecke.
"""

import os

from sqlalchemy import Column, String, Table
from sqlalchemy.orm import sessionmaker

from data_processing_utils import insert_data
from db_utils import get_db_engine, get_metadata

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()


# Define seasons
seasons = ["20152016", "20162017", "20172018"]


# Function to create player cap hit tables
def create_caphit_table(table_name):
    """Define table creation function to avoid repetition."""
    return Table(
        table_name,
        metadata,
        Column("firstName", String(50)),
        Column("lastName", String(50)),
        Column("capHit", String(50)),
    )


# Create tables for each season
tables = {season: create_caphit_table(f"player_cap_hit_{season}") for season in seasons}
metadata.create_all(engine)

# Initialize database session
Session = sessionmaker(bind=engine)

# Dynamically construct file paths (makes the code portable)
base_dir = os.getcwd()  # Get the current working directory
csv_dir = os.path.join(base_dir, "player_cap_hits")  # Path to CSV directory

# Define CSV file paths and table names dynamically
csv_files_and_mappings = [
    (os.path.join(csv_dir, f"player_cap_hits_{year}.csv"), f"player_cap_hit_{season}")
    for year, season in zip([2015, 2016, 2017], seasons)
]

# Insert data from CSV files
with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if os.path.exists(file_path):  # Ensure the file exists before inserting
            insert_data(engine, table_name, file_path)
        else:
            print(f"File not found: {file_path}, skipping...")

print("Data inserted successfully into all tables.")
