"""
game_plays_players_processor.py.

This script downloads, extracts, cleans, and inserts `game_plays_players` data
from AWS S3 into a PostgreSQL database.

Refactored to use:
- `s3_utils.py` for S3 operations.
- `data_processing_utils.py` for data cleaning, extraction, and database handling.

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os
import string

import pandas as pd
from sqlalchemy.orm import sessionmaker

from data_processing_utils import (
    clean_data,
    clear_directory,
    extract_zip,
    insert_data,
)
from db_utils import define_game_plays_players_test, get_db_engine, get_metadata
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

# Define table schema
game_plays_players_test = define_game_plays_players_test(metadata)

# Create the table if it does not exist
metadata.create_all(engine)

# Define column mapping for cleaning
column_mapping = {
    "play_id": "str",
    "game_id": "int64",
    "player_id": "int64",
    "player_type": "str",  # Renamed to match standardized format
}

# AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = "game_plays_players.csv.zip"

# Local Paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game_plays_players.zip")
csv_file_path = os.path.join(local_extract_path, "game_plays_players.csv")


def add_suffix_to_duplicate_play_ids(df):
    """Add alphabetical suffixes to duplicate 'play_id' values to ensure uniqueness."""
    # Debugging output for column names
    print(
        "Columns in DataFrame when entering add_suffix_to_duplicate_play_ids:",
        list(df.columns),
    )

    # Verify the existence of 'play_id' column before proceeding
    if "play_id" not in df.columns:
        raise KeyError("The 'play_id' column is missing in the DataFrame!")

    # Convert 'play_id' to string and fill missing values
    df["play_id"] = df["play_id"].astype(str).fillna("MISSING_ID")

    play_id_counts = {}  # Dictionary to track occurrences of each play_id

    # Iterate over the DataFrame index to avoid issues with Series indexing
    for idx in df.index:
        play_id = df.at[idx, "play_id"]  # Access play_id directly by index
        logging.debug(f"Processing play_id: {play_id}")  # Debug log for each play_id

        # Check if play_id has already been seen
        if play_id in play_id_counts:
            # Increment count and generate a suffix safely
            play_id_counts[play_id] += 1
            suffix_index = play_id_counts[play_id] - 1

            # Handle cases where more than 26 duplicates exist
            if suffix_index < 26:
                suffix = string.ascii_lowercase[suffix_index]
            else:
                suffix = f"_{suffix_index}"  # Numeric fallback beyond 'z'

            df.at[idx, "play_id"] = f"{play_id}{suffix}"
            logging.debug(
                f"Updated play_id: {df.at[idx, 'play_id']}"
            )  # Log updated play_id
        else:
            # Initialize the count for this play_id
            play_id_counts[play_id] = 1

    return df


def process_and_clean_data(file_path, column_mapping):
    """Load, verify, and clean game_plays_players CSV data."""
    # Step 1: Read CSV
    df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

    # Log raw data before cleaning
    logging.info(f"Raw Data Sample Before Cleaning:\n{df.head()}")
    logging.info(f"CSV Columns Before Processing: {list(df.columns)}")

    # Step 2: Apply Generic Cleaning
    df = clean_data(df, column_mapping)

    # Step 3: Drop rows with missing play_id or playerType (specific to game_plays_players)
    initial_row_count = len(df)
    df = df.dropna(subset=["play_id", "playerType"])
    logging.info(
        f"Dropped {initial_row_count - len(df)} rows with missing 'play_id' or 'playerType'."
    )

    # Step 4: Ensure unique play_ids by adding suffixes to duplicates
    df = add_suffix_to_duplicate_play_ids(df)

    # Step 5: Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    # Log cleaned data
    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    return df  # ✅ Now with unique play_ids


def process_and_insert_data():
    """Execute downloading, extracting, cleaning, and inserting data into the test table."""
    session = Session()

    # Step 1: Clear old extraction folder
    clear_directory(local_extract_path)

    # Step 2: Download ZIP from S3
    download_from_s3(bucket_name, s3_file_key, local_zip_path)

    # Step 3: Extract ZIP file
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    if "game_plays_players.csv" not in extracted_files:
        logging.error(f"Missing expected file: {csv_file_path}")
        return

    # Step 4: Ensure test table exists
    metadata.create_all(engine)  # Ensure `game_plays_players_test` exists
    metadata.reflect(bind=engine)  # Refresh metadata
    game_plays_players_test = metadata.tables.get("game_plays_players_test")

    if game_plays_players_test is None:
        logging.error("Table game_plays_players_test does not exist after reflection.")
        return

    # Step 5: Process and clean data
    df = process_and_clean_data(csv_file_path, column_mapping)

    # Step 6: Log the cleaned data and insert into the test database table
    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    try:
        logging.info("Inserting data into table: game_plays_players_test")
        insert_data(df, game_plays_players_test, session)  # ✅ Correct table
        logging.info("Data successfully inserted into game_plays_players.")
    except Exception as e:
        logging.error(
            f"Error inserting data into game_plays_players: {e}", exc_info=True
        )

    session.close()
    logging.info("Processing completed successfully.")


# Run processing in a Jupyter Notebook-friendly manner
if __name__ == "__main__":
    process_and_insert_data()
