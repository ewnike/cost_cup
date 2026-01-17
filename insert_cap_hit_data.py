"""
August 11, 2024.

Insert Spotrac cap hit data into hockey_stats database.

Author: Eric Winiecke.
"""

import os

import pandas as pd
from sqlalchemy.orm import sessionmaker

from config_helpers import COLUMN_MAPPINGS
from constants import SEASONS_MODERN
from data_processing_utils import insert_data
from db_utils import create_caphit_table, get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

base_dir = os.getcwd()
csv_dir = os.path.join(base_dir, "player_cap_hits")

# Map CSV years -> season table names
csv_files_and_mappings = [
    (os.path.join(csv_dir, f"player_cap_hits_{year}.csv"), f"player_cap_hit_{season}")
    for year, season in zip([2018, 2019, 2020, 2021, 2022, 2023, 2024], SEASONS_MODERN)
]

# Create all tables once
for _, table_name in csv_files_and_mappings:
    create_caphit_table(table_name, metadata)
metadata.create_all(engine)

for file_path, table_name in csv_files_and_mappings:
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}, skipping...")
        continue

    season_id = int(table_name.split("_")[-1])  # 20182019, etc.
    table = create_caphit_table(table_name, metadata)
    valid_cols = set(table.columns.keys())

    # ---- Read CSV ONCE ----
    df = pd.read_csv(file_path)
    df.columns = [c.strip() for c in df.columns]

    # If table expects spotrac_url but CSV doesn't have it (older seasons)
    if "spotrac_url" in valid_cols and "spotrac_url" not in df.columns:
        df["spotrac_url"] = None

    # If table expects season (ONLY if you truly kept season in schema)
    if "season" in valid_cols and "season" not in df.columns:
        df["season"] = season_id

    # Drop any extra cols not in the table (prevents "Unconsumed column names")
    extra_cols = [c for c in df.columns if c not in valid_cols]
    if extra_cols:
        df = df.drop(columns=extra_cols)

    # Reorder to schema order (optional, but nice)
    df = df[[c for c in table.columns.keys() if c in df.columns]].copy()

    # IMPORTANT: your insert_data() closes the session, so use a NEW session per table
    session = Session()
    try:
        insert_data(df, table, session, column_mapping=COLUMN_MAPPINGS["cap_hit"])
    finally:
        # insert_data already closes, but this keeps it clean if you later remove that close()
        try:
            session.close()
        except Exception:
            pass

print("Cap hit data inserted.")

# after loop + after session commit(s) succeeded
clear_player_cap_hits_dir(csv_dir)

engine.dispose()
