"""
game_shifts_processor.py.

Downloads, extracts, cleans, and inserts `game_shifts` data
from AWS S3 into a PostgreSQL test database table.

Refactored to use:
- `config_helpers.py` for config generation
- `data_processing_utils.py` for the ETL pipeline
- `log_utils.py` for standardized logging

Author: Eric Winiecke
Date: July 2025
"""

from .config_helpers import game_shifts_config
from .data_processing_utils import process_and_insert_data
from .log_utils import setup_logger

# ✅ Set up logging
setup_logger()

# ✅ Get predefined config and run the ETL pipeline
config = game_shifts_config()
process_and_insert_data(config)
