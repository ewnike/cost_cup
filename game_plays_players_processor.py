"""
game_plays_players_processor.py.

This script downloads, extracts, cleans, and inserts `game_plays_players` data
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
import string

from data_processing_utils import process_and_insert_data
from db_utils import define_game_plays_players_test, get_db_engine, get_metadata

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ✅ Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# ✅ Define column mapping for cleaning
game_plays_players_column_mapping = {
    "play_id": "str",
    "game_id": "int64",
    "player_id": "int64",
    "playerType": "str",  # Standardized format
}

# ✅ AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = "game_plays_players.csv.zip"

# ✅ Local Paths
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
local_zip_path = os.path.join(local_download_path, "game_plays_players.zip")
csv_file_path = os.path.join(local_extract_path, "game_plays_players.csv")


def add_suffix_to_duplicate_play_ids(df):
    """Only add suffixes to truly duplicated 'play_id' values."""
    if "play_id" not in df.columns:
        raise KeyError("The 'play_id' column is missing in the DataFrame!")

    # ✅ Log unique play_ids before processing
    logging.info(f"Before Suffix Addition - Unique play_ids: {df['play_id'].nunique()}")

    # Identify duplicates (rows with the same play_id and player_id)
    duplicate_mask = df.duplicated(subset=["play_id", "player_id"], keep=False)

    # Only process rows where play_id is truly duplicated
    df_duplicates = df[duplicate_mask].copy()

    if not df_duplicates.empty:
        play_id_counts = {}

        for idx in df_duplicates.index:
            play_id = df.at[idx, "play_id"]

            if play_id in play_id_counts:
                play_id_counts[play_id] += 1
                suffix_index = play_id_counts[play_id] - 1

                if suffix_index < 26:
                    suffix = string.ascii_lowercase[suffix_index]
                else:
                    suffix = f"_{suffix_index}"

                df.at[idx, "play_id"] = f"{play_id}{suffix}"
            else:
                play_id_counts[play_id] = 1

    # ✅ Log unique play_ids after processing
    logging.info(f"After Suffix Addition - Unique play_ids: {df['play_id'].nunique()}")

    return df


def process_and_clean_data(file_path, column_mapping):
    """Load, clean, and standardize game_plays_players CSV data."""
    import pandas as pd

    # Step 1: Read CSV
    df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

    # ✅ Log raw data before processing
    logging.info(f"Extracted Rows Before Processing: {len(df)}")
    logging.info(f"CSV Columns Before Processing: {list(df.columns)}")

    # Step 2: Apply Generic Cleaning (without dropping duplicates)
    from data_processing_utils import clean_data

    df = clean_data(df, column_mapping, drop_duplicates=False)  # ✅ Do NOT drop duplicates

    # ✅ Step 3: Drop rows ONLY IF `play_id` or `playerType` is completely missing
    initial_row_count = len(df)
    df = df.dropna(
        subset=["play_id", "playerType"], how="all"
    )  # ✅ "all" ensures full NaN rows are dropped
    logging.info(
        f"Dropped {initial_row_count - len(df)} rows with fully missing 'play_id' or 'playerType'."
    )

    # ✅ Step 4: Ensure unique play_ids by adding suffixes BEFORE duplicate removal
    df = add_suffix_to_duplicate_play_ids(df)

    # ✅ Step 5: Only remove duplicate (play_id, player_id) pairs, not full rows
    df = df.drop_duplicates(subset=["play_id", "player_id"], ignore_index=True)

    return df  # Unique play_ids, all player rows retained


# ✅ Run the standardized `process_and_insert_data()` function
process_and_insert_data(
    bucket_name=bucket_name,
    s3_file_key=s3_file_key,
    local_zip_path=local_zip_path,
    local_extract_path=local_extract_path,
    expected_csv_filename="game_plays_players.csv",
    table_definition_function=define_game_plays_players_test,
    table_name="game_plays_players_test",
    column_mapping=game_plays_players_column_mapping,
    engine=engine,
)
