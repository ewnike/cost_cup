"""
Insert team event totals data into hockey_stats database.

Author: Eric Winiecke
Date: January 5, 2025
"""

from pathlib import Path

import pandas as pd
from sqlalchemy.orm import sessionmaker

from .config_helpers import COLUMN_MAPPINGS
from .constants import SEASONS_ACTIVE as SEASONS
from .data_processing_utils import insert_data
from .db_utils import create_team_event_total_games_table, get_db_engine, get_metadata
from .log_utils import setup_logger

# ✅ Init
logger = setup_logger()
engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

# ✅ Dynamically determine the base directory
BASE_DIR = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
DATA_DIR = BASE_DIR / "team_event_totals"

if not DATA_DIR.exists():
    logger.error(f"Directory not found: {DATA_DIR}")
    raise FileNotFoundError(f"Directory {DATA_DIR} does not exist.")

# ✅ Create tables if needed
tables = {
    season: create_team_event_total_games_table(f"team_event_totals_games_{season}", metadata)
    for season in SEASONS
}
metadata.create_all(engine)

# ✅ Define CSV files and matching table names
csv_files_and_mappings = [
    (DATA_DIR / f"team_event_totals_games_{season}.csv", f"team_event_totals_games_{season}")
    for season in SEASONS
]

# ✅ Insert data
with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if file_path.exists():
            df = pd.read_csv(file_path)
            table = create_team_event_total_games_table(table_name, metadata)
            insert_data(
                df, table, session, column_mapping=COLUMN_MAPPINGS["team_event_totals_games"]
            )
            file_path.unlink()  # Remove the file after successful insertion
            logger.info(f"File {file_path} deleted successfully.")
        else:
            logger.warning(f"File not found, skipping: {file_path}")

logger.info("Data inserted successfully into all tables.")
