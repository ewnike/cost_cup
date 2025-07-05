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

import os

from data_processing_utils import process_and_insert_data
from db_utils import define_game_plays_players_test, get_db_engine, get_metadata
from log_utils import setup_logger

setup_logger()


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
S3_FILE_KEY = "game_plays_players.csv.zip"

# ✅ Local Paths
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
LOCAL_ZIP_PATH = os.path.join(local_download_path, "game_plays_players.zip")
csv_file_path = os.path.join(local_extract_path, "game_plays_players.csv")


# Decide whether to process a ZIP file or a direct CSV/XLS file
HANDLE_ZIP = bool(LOCAL_ZIP_PATH)  # True if local_zip_path is not empty
print(bool(LOCAL_ZIP_PATH))

config = {
    "bucket_name": bucket_name,
    "s3_file_key": S3_FILE_KEY,
    "local_zip_path": LOCAL_ZIP_PATH,
    "local_extract_path": local_extract_path,
    "expected_csv_filename": "game_plays_players.csv",
    "table_definition_function": define_game_plays_players_test,
    "table_name": "game_plays_players_test",
    "column_mapping": game_plays_players_column_mapping,
    "engine": engine,
    "handle_zip": HANDLE_ZIP,
    "local_download_path": local_download_path,
}
# ✅ Run the standardized `process_and_insert_data()` function
process_and_insert_data(config)
