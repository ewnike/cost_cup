"""
August 11, 2024.

Insert Spotrac cap hit data into hockey_stats database.

Author: Eric Winiecke.
"""

import os

import pandas as pd
from sqlalchemy.orm import sessionmaker

from config_helpers import COLUMN_MAPPINGS
from constants import SEASONS
from data_processing_utils import insert_data
from db_utils import create_caphit_table, get_db_engine, get_metadata

# ✅ Init
engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

# ✅ Create tables if needed
tables = {season: create_caphit_table(f"player_cap_hit_{season}", metadata) for season in SEASONS}
metadata.create_all(engine)

# ✅ Build file paths
base_dir = os.getcwd()
csv_dir = os.path.join(base_dir, "player_cap_hits")

csv_files_and_mappings = [
    (os.path.join(csv_dir, f"player_cap_hits_{year}.csv"), f"player_cap_hit_{season}")
    for year, season in zip([2015, 2016, 2017], SEASONS)
]

# ✅ Insert data
with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            table = create_caphit_table(table_name, metadata)
            insert_data(df, table, session, column_mapping=COLUMN_MAPPINGS["cap_hit"])

        else:
            print(f"File not found: {file_path}, skipping...")

print("Data inserted successfully into all tables.")
