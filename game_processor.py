"""
game_processor.py.

This script downloads, extracts, cleans, and inserts `game` data
from AWS S3 into a PostgreSQL test table.

Refactored to use:
- `s3_utils.py` for S3 operations.
- `data_processing_utils.py` for data cleaning, extraction, and database handling.
- `db_utils.py` for database connection management.

Author: Eric Winiecke
Date: February 2025
"""

import os

from data_processing_utils import process_and_insert_data
from db_utils import define_game_table_test, get_db_engine, get_metadata
from log_utils import setup_logging

setup_logging()

# ✅ Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# ✅ Define column mapping for cleaning
game_column_mapping = {
    "game_id": "int64",
    "season": "int64",
    "type": "str",
    "date_time_GMT": "datetime64",
    "away_team_id": "int64",
    "home_team_id": "int64",
    "away_goals": "int64",
    "home_goals": "int64",
    "outcome": "str",
    "home_rink_side_start": "str",
    "venue": "str",
    "venue_link": "str",
    "venue_time_zone_id": "str",
    "venue_time_zone_offset": "int64",
    "venue_time_zone_tz": "str",
}

# ✅ AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "game.csv.zip"

# ✅ Local Paths
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
LOCAL_ZIP_PATH = os.path.join(local_download_path, "game.zip")
csv_file_path = os.path.join(local_extract_path, "game.csv")

# Decide whether to process a ZIP file or a direct CSV/XLS file
HANDLE_ZIP = bool(LOCAL_ZIP_PATH)  # True if local_zip_path is not empty
print(bool(LOCAL_ZIP_PATH))

config = {
    "bucket_name": bucket_name,
    "s3_file_key": S3_FILE_KEY,
    "local_zip_path": LOCAL_ZIP_PATH,
    "local_extract_path": local_extract_path,
    "expected_csv_filename": "game.csv",
    "table_definition_function": define_game_table_test,
    "table_name": "game_table_test",
    "column_mapping": game_column_mapping,
    "engine": engine,
    "handle_zip": HANDLE_ZIP,
    "local_download_path": local_download_path,
}
# ✅ Run the standardized `process_and_insert_data()` function
process_and_insert_data(config)
