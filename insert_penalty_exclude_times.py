"""
January 5, 2025.

Code to create datatables and insert
data into the penalty_exclude_times
tables for each season.

Eric Winiecke
"""

import logging
import os  # noqa: F401
from pathlib import Path

import pandas as pd
from sqlalchemy import (
    VARCHAR,
    BigInteger,
    Column,
    Integer,
    Table,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from db_utils import get_db_engine, get_metadata

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize DB Engine and Metadata
engine = get_db_engine()
metadata = get_metadata()

# Dynamically get the directory where this script is located
BASE_DIR = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
DATA_DIR = BASE_DIR / "team_event_totals"

# Ensure the directory exists
if not DATA_DIR.exists():
    logging.error(f"Directory not found: {DATA_DIR}")
    raise FileNotFoundError(f"Directory {DATA_DIR} does not exist.")


def create_penalty_exclude_table(table_name):
    """
    Create a penalty exclude times table for a given season.

    Args:
    ----
        table_name (str): The name of the table to create.

    Returns:
    -------
        sqlalchemy.Table: The SQLAlchemy table object.

    """
    return Table(
        table_name,
        metadata,
        Column("time", Integer),
        Column("team_1", Integer),
        Column("team_2", Integer),
        Column("game_id", BigInteger),
        Column("exclude", VARCHAR(5)),
    )


# Define seasons and create tables dynamically
seasons = ["20152016", "20162017", "20172018"]
tables = {
    season: create_penalty_exclude_table(f"penalty_exclude_times_{season}") for season in seasons
}

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)


def insert_data_from_csv(engine, table_name, file_path):
    """
    Insert data from a CSV file into the specified database table.

    Args:
    ----
        engine (sqlalchemy.Engine): The database engine.
        table_name (str): The name of the table to insert data into.
        file_path (Path): The path to the CSV file.

    Raises:
    ------
        SQLAlchemyError: If an error occurs while inserting data.
        FileNotFoundError: If the CSV file is not found.

    """
    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info(f"Data inserted successfully into {table_name}")

        # Remove the file after successful insertion
        file_path.unlink()
        logging.info(f"File {file_path} deleted successfully.")

    except SQLAlchemyError as e:
        logging.error(f"Error inserting data into {table_name}: {e}")
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path} - {e}")
    except Exception as e:
        logging.error(f"Error occurred while processing file '{file_path}': {e}")


# Generate file paths dynamically
csv_files_and_mappings = [
    (DATA_DIR / f"penalty_exclude_times_{season}.csv", f"penalty_exclude_times_{season}")
    for season in seasons
]

# Insert data for each season
for file_path, table_name in csv_files_and_mappings:
    if file_path.exists():
        insert_data_from_csv(engine, table_name, file_path)
    else:
        logging.warning(f"File not found, skipping: {file_path}")

logging.info("Data inserted successfully into all tables.")
