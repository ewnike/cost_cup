# """
# January 5, 2025.

# Code to create datatables and insert
# data into the team_event_total_games
# tables for each season.

# Eric Winiecke
# """

# import logging
# from pathlib import Path

# import pandas as pd
# from sqlalchemy import (
#     BigInteger,
#     Column,
#     Integer,
#     Table,
# )
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.orm import sessionmaker

# from constants import SEASONS
# from db_utils import get_db_engine, get_metadata

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # Initialize DB Engine and Metadata
# engine = get_db_engine()
# metadata = get_metadata()

# # Dynamically determine the base directory
# BASE_DIR = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
# DATA_DIR = BASE_DIR / "team_event_totals"

# # Ensure the directory exists
# if not DATA_DIR.exists():
#     logging.error(f"Directory not found: {DATA_DIR}")
#     raise FileNotFoundError(f"Directory {DATA_DIR} does not exist.")


# def create_team_event_total_games_table(table_name):
#     """
#     Create a team event totals table for a given season.

#     Args:
#     ----
#         table_name (str): The name of the table to create.

#     Returns:
#     -------
#         sqlalchemy.Table: The SQLAlchemy table object.

#     """
#     return Table(
#         table_name,
#         metadata,
#         Column("team_id", Integer),
#         Column("total_goals", Integer),
#         Column("total_shots", Integer),
#         Column("total_missed_shots", Integer),
#         Column("total_blocked_shots_for", Integer),
#         Column("total_goals_against", Integer),
#         Column("total_shots_against", Integer),
#         Column("total_missed_shots_against", Integer),
#         Column("total_blocked_shots_against", Integer),
#         Column("game_id", BigInteger),
#     )


# # Define seasons and create tables dynamically
# # seasons = ["20152016", "20162017", "20172018"]
# tables = {
#     season: create_team_event_total_games_table(f"team_event_totals_games_{season}")
#     for season in SEASONS
# }  # noqa: E501

# # Create tables in the database
# metadata.create_all(engine)

# Session = sessionmaker(bind=engine)


# def insert_data_from_csv(engine, table_name, file_path):
#     """
#     Insert data from a CSV file into the specified database table.

#     Args:
#     ----
#         engine (sqlalchemy.Engine): The database engine.
#         table_name (str): The name of the table to insert data into.
#         file_path (Path): The path to the CSV file.

#     Raises:
#     ------
#         SQLAlchemyError: If an error occurs while inserting data.
#         FileNotFoundError: If the CSV file is not found.

#     """
#     try:
#         df = pd.read_csv(file_path)
#         df.to_sql(table_name, con=engine, if_exists="append", index=False)
#         logging.info(f"Data inserted successfully into {table_name}")

#         # Remove the file after successful insertion
#         file_path.unlink()
#         logging.info(f"File {file_path} deleted successfully.")

#     except SQLAlchemyError as e:
#         logging.error(f"Error inserting data into {table_name}: {e}")
#     except FileNotFoundError as e:
#         logging.error(f"File not found: {file_path} - {e}")
#     except Exception as e:
#         logging.error(f"Error occurred while processing file '{file_path}': {e}")


# # Generate file paths dynamically
# csv_files_and_mappings = [
#     (DATA_DIR / f"team_event_totals_games_{season}.csv", f"team_event_totals_games_{season}")
#     for season in SEASONS
# ]

# # Insert data for each season
# for file_path, table_name in csv_files_and_mappings:
#     if file_path.exists():
#         insert_data_from_csv(engine, table_name, file_path)
#     else:
#         logging.warning(f"File not found, skipping: {file_path}")

# logging.info("Data inserted successfully into all tables.")
"""
Insert team event totals data into hockey_stats database.

Author: Eric Winiecke
Date: January 5, 2025
"""

from pathlib import Path

import pandas as pd
from sqlalchemy.orm import sessionmaker

from config_helpers import COLUMN_MAPPINGS
from constants import SEASONS
from data_processing_utils import insert_data
from db_utils import create_team_event_total_games_table, get_db_engine, get_metadata
from log_utils import setup_logger

# ✅ Init
logger = setup_logger()
engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

# ✅ Dynamically determine the base directory
BASE_DIR = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
DATA_DIR = BASE_DIR / "team_event_totals"

if not DATA_DIR.exists():
    logger.error(f"Directory not found: {DATA_DIR}")
    raise FileNotFoundError(f"Directory {DATA_DIR} does not exist.")

# ✅ Create tables if needed
tables = {
    season: create_team_event_total_games_table(f"team_event_totals_games_{season}", metadata)
    for season in SEASONS
}
metadata.create_all(engine)

# ✅ Define CSV files and matching table names
csv_files_and_mappings = [
    (DATA_DIR / f"team_event_totals_games_{season}.csv", f"team_event_totals_games_{season}")
    for season in SEASONS
]

# ✅ Insert data
with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if file_path.exists():
            df = pd.read_csv(file_path)
            table = create_team_event_total_games_table(table_name, metadata)
            insert_data(
                df, table, session, column_mapping=COLUMN_MAPPINGS["team_event_totals_games"]
            )
            file_path.unlink()  # Remove the file after successful insertion
            logger.info(f"File {file_path} deleted successfully.")
        else:
            logger.warning(f"File not found, skipping: {file_path}")

logger.info("Data inserted successfully into all tables.")
