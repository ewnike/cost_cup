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

from data_processing_utils import clean_data, process_and_insert_data
from db_utils import define_game_plays_processor_test, get_db_engine, get_metadata
from log_utils import setup_logger

setup_logger()

# ✅ Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# ✅ Define column mapping for cleaning
game_plays_column_mapping = {
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

# ✅ AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "game_plays.csv.zip"

# ✅ Local Paths
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
LOCAL_ZIP_PATH = os.path.join(local_download_path, "game_plays.zip")
csv_file_path = os.path.join(local_extract_path, "game_plays.csv")


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
    df["x"] = df["x"].fillna(0) if "x" in df.columns else df.get("x", 0)
    df["y"] = df["y"].fillna(0) if "y" in df.columns else df.get("y", 0)

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


# Decide whether to process a ZIP file or a direct CSV/XLS file
HANDLE_ZIP = bool(LOCAL_ZIP_PATH)  # True if local_zip_path is not empty

config = {
    "bucket_name": bucket_name,
    "s3_file_key": S3_FILE_KEY,
    "local_zip_path": LOCAL_ZIP_PATH,
    "local_extract_path": local_extract_path,
    "expected_csv_filename": "game_plays.csv",
    "table_definition_function": define_game_plays_processor_test,
    "table_name": "game_plays_processor_test",
    "column_mapping": game_plays_column_mapping,
    "engine": engine,
    "handle_zip": HANDLE_ZIP,
    "local_download_path": local_download_path,
}
# ✅ Run the standardized `process_and_insert_data()` function
process_and_insert_data(config)
