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

from data_processing_utils import process_and_insert_data
from db_utils import define_game_shifts_test_table, get_db_engine, get_metadata

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
game_shifts_column_mapping = {
    "game_id": "int64",
    "player_id": "int64",
    "period": "int64",
    "shift_start": "int64",
    "shift_end": "int64",
}

# ✅ AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = "game_shifts.csv.zip"

# ✅ Local Paths (Use absolute paths instead of `/path/to`)
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
local_zip_path = os.path.join(local_download_path, "game_shifts.zip")
csv_file_path = os.path.join(local_extract_path, "game_shifts.csv")

# ✅ Run the generic `process_and_insert_data()` function
process_and_insert_data(
    bucket_name=bucket_name,
    s3_file_key=s3_file_key,
    local_zip_path=local_zip_path,  # ✅ ZIP goes into `data/download/`
    local_extract_path=local_extract_path,  # ✅ Extracted CSV goes into `data/extracted/`
    expected_csv_filename="game_shifts.csv",
    table_definition_function=define_game_shifts_test_table,
    table_name="game_shifts_test",
    column_mapping=game_shifts_column_mapping,
    engine=engine,
)
