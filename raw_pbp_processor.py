"""
raw_shifts_processor.py.

Downloads, extracts, cleans, and inserts `raw_shifts` data
from AWS S3 into a PostgreSQL test database table.

Refactored to use:
- `config_helpers.py` for config generation
- `data_processing_utils.py` for the ETL pipeline
- `log_utils.py` for standardized logging

Author: Eric Winiecke
Date: December 2025
"""

from config_helpers import pbp_raw_data_config
from data_processing_utils import process_and_insert_data
from log_utils import setup_logger

# ✅ Set up logging
setup_logger()

# ✅ Get predefined config and run the ETL pipeline
for season in [20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025]:
    config = pbp_raw_data_config(season)
    print("expected_csv_filename:", config.get("expected_csv_filename"))
    print("s3_file_key:", config.get("s3_file_key"))
    print("table_name:", config.get("table_name"))

    process_and_insert_data(config)
