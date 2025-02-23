"""
game_skater_stats_processor.py.

This script downloads, extracts, cleans, and inserts `game_skater_stats` data
from AWS S3 into a PostgreSQL test database table.

Refactored to use:
- `s3_utils.py` for S3 operations.
- `data_processing_utils.py` for data cleaning, extraction, and database handling.
- `db_utils.py` for database connections and table creation.

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os

import pandas as pd
from sqlalchemy.orm import sessionmaker

from data_processing_utils import (
    clean_data,
    clear_directory,
    extract_zip,
    insert_data,
)
from db_utils import define_game_skater_stats_test, get_db_engine, get_metadata
from s3_utils import download_from_s3

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

# Table Name for Testing
TABLE_NAME = "game_skater_stats_test"

# Define column mapping for cleaning
game_skater_stats_column_mapping = {
    "game_id": "int64",
    "player_id": "int64",
    "team_id": "int64",
    "timeOnIce": "int64",
    "assists": "int64",
    "goals": "int64",
    "shots": "int64",
    "hits": "int64",
    "powerPlayGoals": "int64",
    "powerPlayAssists": "int64",
    "penaltyMinutes": "int64",
    "faceOffWins": "int64",
    "faceoffTaken": "int64",
    "takeaways": "int64",
    "giveaways": "int64",
    "shortHandedGoals": "int64",
    "shortHandedAssists": "int64",
    "blocked": "int64",
    "plusMinus": "int64",
    "evenTimeOnIce": "int64",
    "shortHandedTimeOnIce": "int64",
    "powerPlayTimeOnIce": "int64",
}

# AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "game_skater_stats.csv.zip"

# Local Paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game_skater_stats.zip")
csv_file_path = os.path.join(local_extract_path, "game_skater_stats.csv")


def process_and_clean_data(file_path, column_mapping):
    """Load, clean, and standardize the game_skater_stats CSV data."""
    df = pd.read_csv(file_path, dtype=str)
    logging.info(f"Columns in loaded CSV: {list(df.columns)}")

    # Standardize column names (lowercase and remove spaces)
    df.columns = df.columns.str.strip().str.lower()

    # # Convert expected column names to lowercase for comparison
    # expected_columns = set([col.lower() for col in column_mapping.keys()])
    # actual_columns = set(df.columns)

    # missing_cols = expected_columns - actual_columns
    # if missing_cols:
    #     raise KeyError(f"Missing expected columns: {missing_cols}")

    # Convert expected column names to lowercase for comparison
    expected_columns = {col.lower() for col in column_mapping.keys()}  # Using set comprehension
    actual_columns = set(df.columns)

    missing_cols = expected_columns - actual_columns
    if missing_cols:
        raise KeyError(f"Missing expected columns: {missing_cols}")

    # Rename columns in the DataFrame to match the expected format
    df.rename(columns={col.lower(): col for col in column_mapping.keys()}, inplace=True)

    # Apply generic cleaning
    df = clean_data(df, column_mapping)

    # Drop rows with missing essential values
    initial_row_count = len(df)
    df = df.dropna(subset=["game_id", "player_id", "team_id"])
    logging.info(f"Dropped {initial_row_count - len(df)} rows with missing essential values.")

    # Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    return df


def process_and_insert_data():
    """Execute downloading, extracting, cleaning, and inserting data into the test table."""
    session = Session()

    # Step 1: **Clear Old Data**
    clear_directory(local_extract_path)

    # Step 2: **Download and Extract Data**
    download_from_s3(bucket_name, S3_FILE_KEY, local_zip_path)
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    if "game_skater_stats.csv" not in extracted_files:
        logging.error(f"Missing expected file: {csv_file_path}")
        return

    # Step 3: **Ensure Table Exists**
    game_skater_stats_test = define_game_skater_stats_test(metadata)
    metadata.create_all(engine)  # Ensure the table is created
    metadata.reflect(bind=engine)  # Refresh metadata

    if "game_skater_stats_test" not in metadata.tables:
        logging.error("Table game_skater_stats_test does not exist after reflection.")
        return

    # Step 4: **Process and Clean Data**
    df = process_and_clean_data(csv_file_path, game_skater_stats_column_mapping)

    # Step 5: **Insert Data**
    try:
        logging.info("Inserting data into table: game_skater_stats_test")
        insert_data(df, game_skater_stats_test, session)
        logging.info("Data successfully inserted into game_skater_stats_test.")
    except Exception as e:
        logging.error(f"Error inserting data into game_skater_stats_test: {e}", exc_info=True)

    session.close()
    logging.info("Processing completed successfully.")

    # Step 6: **Clean Up Extracted Data**
    clear_directory(local_extract_path)


if __name__ == "__main__":
    process_and_insert_data()
