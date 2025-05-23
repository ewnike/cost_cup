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

import os

from data_processing_utils import process_and_insert_data
from db_utils import define_game_skater_stats_test, get_db_engine, get_metadata
from log_utils import setup_logging

setup_logging()

# ✅ Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# ✅ Define column mapping for cleaning
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

# ✅ AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "game_skater_stats.csv.zip"

# Local Paths (Use an absolute path instead of /path/to)
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
LOCAL_ZIP_PATH = os.path.join(local_download_path, "game_skater_stats.zip")
csv_file_path = os.path.join(local_extract_path, "game_skater_stats.csv")

# Decide whether to process a ZIP file or a direct CSV/XLS file
HANDLE_ZIP = bool(LOCAL_ZIP_PATH)  # True if local_zip_path is not empty
print(bool(LOCAL_ZIP_PATH))

config = {
    "bucket_name": bucket_name,
    "s3_file_key": S3_FILE_KEY,
    "local_zip_path": LOCAL_ZIP_PATH,
    "local_extract_path": local_extract_path,
    "expected_csv_filename": "game_skater_stats.csv",
    "table_definition_function": define_game_skater_stats_test,
    "table_name": "game_skater_stats_test",
    "column_mapping": game_skater_stats_column_mapping,
    "engine": engine,
    "handle_zip": HANDLE_ZIP,
    "local_download_path": local_download_path,
}
# ✅ Run the standardized `process_and_insert_data()` function
process_and_insert_data(config)
