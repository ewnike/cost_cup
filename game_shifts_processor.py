"""
game_shifts_processor.py.

This script downloads, extracts, cleans, and inserts `game_shifts` data
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
from db_utils import define_game_shifts_test_table, get_db_engine, get_metadata
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
TABLE_NAME = "game_shifts_test"

# Define column mapping for cleaning
game_shifts_column_mapping = {
    "game_id": "int64",
    "player_id": "int64",
    "period": "int64",
    "shift_start": "int64",
    "shift_end": "int64",
}

# AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "game_shifts.csv.zip"

# Local Paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game_shifts.zip")
csv_file_path = os.path.join(local_extract_path, "game_shifts.csv")


def process_and_clean_data(file_path, column_mapping):
    """Load, clean, and standardize the game_shifts CSV data."""
    df = pd.read_csv(file_path, dtype=str)
    logging.info(f"Columns in loaded CSV: {list(df.columns)}")

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # Check for missing expected columns
    expected_columns = set(column_mapping.keys())
    missing_cols = expected_columns - set(df.columns)
    if missing_cols:
        raise KeyError(f"Missing expected columns: {missing_cols}")

    # Apply generic cleaning
    df = clean_data(df, column_mapping)

    # Drop rows with missing essential values
    initial_row_count = len(df)
    df = df.dropna(subset=["game_id", "player_id", "shift_start", "shift_end"])
    logging.info(f"Dropped {initial_row_count - len(df)} rows with missing essential values.")

    # Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    return df


def process_and_insert_data():
    """Execute downloading, extracting, cleaning, and inserting data."""
    session = Session()

    # Step 1: Prepare directories
    clear_directory(local_extract_path)

    # Step 2: Download ZIP from S3
    download_from_s3(bucket_name, S3_FILE_KEY, local_zip_path)

    # Step 3: Extract ZIP file
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    if "game_shifts.csv" not in extracted_files:
        logging.error(f"Missing expected file: {csv_file_path}")
        return

    # Step 4: Define test table from `db_utils.py`
    game_shifts_test = define_game_shifts_test_table(metadata)
    metadata.create_all(engine)  # Ensure the test table exists

    if game_shifts_test is None:
        logging.error(f"Table {TABLE_NAME} does not exist after reflection.")
        return

    # Step 5: Process and clean data
    df = process_and_clean_data(csv_file_path, game_shifts_column_mapping)

    # Step 6: Insert data into the test database table
    try:
        logging.info(f"Inserting data into table: {TABLE_NAME}")
        insert_data(df, game_shifts_test, session)
        logging.info(f"Data successfully inserted into {TABLE_NAME}.")
    except Exception as e:
        logging.error(f"Error inserting data into {TABLE_NAME}: {e}", exc_info=True)

    session.close()
    logging.info("Processing completed successfully.")


if __name__ == "__main__":
    process_and_insert_data()
