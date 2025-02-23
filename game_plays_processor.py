"""
game_plays_processor.py.

This script downloads, extracts, cleans, and inserts `game_plays` data
from AWS S3 into a PostgreSQL test table.

Refactored to use:
- `s3_utils.py` for S3 operations.
- `data_processing_utils.py` for data cleaning, extraction, and database handling.
- `db_utils.py` for database connection management.

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os

import pandas as pd
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker

from data_processing_utils import (
    clean_data,
    clear_directory,
    ensure_table_exists,
    extract_zip,
    insert_data,
)
from db_utils import get_db_engine, get_metadata
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

# Define test table name
TABLE_NAME = "game_plays_processor_test"

# AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "game_plays.csv.zip"

# Local Paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
local_zip_path = os.path.join(local_extract_path, "game_plays.zip")
csv_file_path = os.path.join(local_extract_path, "game_plays.csv")

# Define column mapping
column_mapping = {
    "play_id": "str",
    "game_id": "int64",
    "team_id_for": "int64",
    "team_id_against": "int64",
    "event": "str",
    "secondaryType": "str",
    "x": "float",
    "y": "float",
    "period": "int64",
    "periodType": "str",
    "periodTime": "int64",
    "periodTimeRemaining": "int64",
    "dateTime": "datetime64",
    "goals_away": "int64",
    "goals_home": "int64",
    "description": "str",
    "st_x": "int64",
    "st_y": "int64",
}


def process_and_clean_data(file_path, column_mapping):
    """Load, verify, and apply game_plays-specific cleaning to the CSV data."""
    # Step 1: Read CSV into DataFrame
    df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

    # Log raw data before cleaning
    logging.info(f"Raw Data Sample Before Cleaning:\n{df.head()}")
    logging.info(f"CSV Columns Before Processing: {list(df.columns)}")

    # Step 2: Apply Generic Cleaning from data_processing_utils
    df = clean_data(df, column_mapping)

    # Step 3: Game-Specific Cleaning
    if "x" in df.columns:
        df["x"] = df["x"].fillna(0)
    if "y" in df.columns:
        df["y"] = df["y"].fillna(0)

    # Truncate long strings and remove whitespace
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].apply(lambda x: str(x).strip()[:255] if isinstance(x, str) else x)

    # Drop rows where 'play_id' or 'event' is missing (specific to game_plays)
    initial_row_count = len(df)
    df = df.dropna(subset=["play_id", "event"])
    logging.info(f"Dropped {initial_row_count - len(df)} rows with missing 'play_id' or 'event'.")

    # Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    # Log cleaned data
    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    return df


def process_and_insert_data():
    """Execute downloading, extracting, cleaning, and inserting data into the test table."""
    session = Session()

    # Step 1: Clear extraction directory
    clear_directory(local_extract_path)

    # Step 2: Download ZIP from S3
    download_from_s3(bucket_name, s3_file_key, local_zip_path)

    # Step 3: Extract ZIP file
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    if "game_plays.csv" not in extracted_files:
        logging.error(f"Missing expected file: {csv_file_path}")
        return

    # Step 4: Ensure the test table exists
    ensure_table_exists(engine, metadata, TABLE_NAME)

    # Step 5: Fetch table reference
    game_plays_test = Table(TABLE_NAME, metadata, autoload_with=engine)

    # Step 6: Process and clean data
    df = process_and_clean_data(csv_file_path, column_mapping)

    # Step 7: Log cleaned data details
    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    try:
        logging.info(f"Inserting data into table: {TABLE_NAME}")
        insert_data(df, game_plays_test, session)
        logging.info(f"Data successfully inserted into {TABLE_NAME}.")
    except Exception as e:
        logging.error(f"Error inserting data into {TABLE_NAME}: {e}", exc_info=True)

    session.close()
    logging.info("Processing completed successfully.")


if __name__ == "__main__":
    process_and_insert_data()
