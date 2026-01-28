"""
August 11, 2024.

Insert newly created raw Corsi data into test tables in the hockey_stats database.

Author: Eric Winiecke
"""

import os

import pandas as pd
from sqlalchemy.orm import sessionmaker

from config_helpers import COLUMN_MAPPINGS
from constants import SEASONS_ACTIVE as SEASONS
from data_processing_utils import insert_data
from db_utils import create_corsi_table, get_db_engine, get_metadata

# ✅ Init
engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

# ✅ Create tables if needed
tables = {season: create_corsi_table(f"raw_corsi_{season}", metadata) for season in SEASONS}
metadata.create_all(engine)

# ✅ Build paths
base_dir = os.getcwd()
csv_dir = os.path.join(base_dir, "corsi_stats")

csv_files_and_mappings = [
    (os.path.join(csv_dir, f"corsi_stats_{season}.csv"), f"raw_corsi_{season}")
    for season in SEASONS
]

# ✅ Insert data
with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            table = create_corsi_table(table_name, metadata)  # This ensures `table` is defined
            insert_data(df, table, session, column_mapping=COLUMN_MAPPINGS["raw_corsi"])
        else:
            print(f"File not found: {file_path}, skipping...")
