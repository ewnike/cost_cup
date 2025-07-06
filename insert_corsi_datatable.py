# """
# August 11, 2024.

# Code for inserting newly created
# Corsi data into defined test tables in the
# hockey_stats database.

# Eric Winiecke.
# """

# import os

# import pandas as pd
# from sqlalchemy import BigInteger, Column, Float, Integer, Table
# from sqlalchemy.orm import sessionmaker

# from config_helpers import COLUMN_MAPPINGS
# from constants import SEASONS
# from data_processing_utils import insert_data
# from db_utils import get_db_engine, get_metadata

# insert_data(engine, table_name, file_path, column_mapping=COLUMN_MAPPINGS["raw_corsi"])

# # Initialize database connection
# engine = get_db_engine()
# metadata = get_metadata()


# # Define seasons
# # seasons = ["20152016", "20162017", "20172018"]


# def create_corsi_table(table_name):
#     """Define table creation function for test tables to avoid overwriting real data."""
#     return Table(
#         table_name,
#         metadata,
#         Column("game_id", BigInteger),
#         Column("player_id", BigInteger),
#         Column("team_id", Integer),
#         Column("corsi_for", Float, nullable=True),
#         Column("corsi_against", Float, nullable=True),
#         Column("corsi", Float, nullable=True),
#         Column("CF_Percent", Float, nullable=True),
#     )


# # Create test tables dynamically (with "_test" suffix)
# tables = {season: create_corsi_table(f"raw_corsi_{season}") for season in SEASONS}
# metadata.create_all(engine)  # Ensure test tables are created

# # Initialize database session
# Session = sessionmaker(bind=engine)

# # Dynamically construct file paths (makes the code portable)
# base_dir = os.getcwd()  # Get the current working directory
# csv_dir = os.path.join(base_dir, "corsi_stats")  # Path to CSV directory

# # Define CSV file paths and test table names dynamically
# csv_files_and_mappings = [
#     (os.path.join(csv_dir, f"corsi_stats_{season}.csv"), f"raw_corsi_{season}")
#     for season in SEASONS
# ]

# # Insert data into test tables
# with Session() as session:
#     for file_path, table_name in csv_files_and_mappings:
#         if os.path.exists(file_path):  # Ensure file exists
#             df = pd.read_csv(file_path)  # Read CSV into DataFrame
#             insert_data(engine, table_name, file_path)

#         else:
#             print(f"File not found: {file_path}, skipping...")


# print("Data inserted successfully into all test tables.")

"""
August 11, 2024.

Insert newly created raw Corsi data into test tables in the hockey_stats database.

Author: Eric Winiecke
"""

import os

import pandas as pd
from sqlalchemy.orm import sessionmaker

from config_helpers import COLUMN_MAPPINGS
from constants import SEASONS
from data_processing_utils import insert_data
from db_utils import create_corsi_table, get_db_engine, get_metadata

# ✅ Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# ✅ Create test tables dynamically: raw_corsi_20152016, etc.
tables = {season: create_corsi_table(f"raw_corsi_{season}", metadata) for season in SEASONS}
metadata.create_all(engine)

# ✅ Construct CSV directory
csv_dir = os.path.join(os.getcwd(), "corsi_stats")

# ✅ Define CSV paths and table names
csv_files_and_mappings = [
    (os.path.join(csv_dir, f"corsi_stats_{season}.csv"), f"raw_corsi_{season}")
    for season in SEASONS
]

# ✅ Insert data into tables
Session = sessionmaker(bind=engine)

with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if os.path.exists(file_path):
            print(f"Inserting data into {table_name} from {file_path}...")
            insert_data(engine, table_name, file_path, column_mapping=COLUMN_MAPPINGS["raw_corsi"])
        else:
            print(f"File not found: {file_path}, skipping...")

print("✅ Data inserted successfully into all raw Corsi test tables.")
