"""
Insert player_game_es_{season} CSVs into Postgres.

Eric Winiecke
January 14, 2026
"""

import os

import pandas as pd
from sqlalchemy.orm import sessionmaker

from .data_processing_utils import insert_data
from .db_utils import create_player_game_es_table, get_db_engine, get_metadata
from .schema_utils import fq

engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

SEASONS = [20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025]

CSV_DIR = os.path.join(os.getcwd(), "player_game_es_csv")  # <-- change if needed
# expects files like: player_game_es_20192020.csv, etc.

REQUIRED_COLS = [
    "game_id",
    "player_id",
    "team_id",
    "cf",
    "ca",
    "toi_sec",
    "cf60",
    "ca60",
    "cf_percent",
]

with Session() as session:
    for season in SEASONS:
        table_name = fq("raw", "player_game_es_{season}")
        table = create_player_game_es_table(table_name, metadata)
        metadata.create_all(engine)

        file_path = os.path.join(CSV_DIR, f"player_game_es_{season}.csv")
        if not os.path.exists(file_path):
            print(f"{season}: missing file {file_path}, skipping")
            continue

        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]

        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"{season}: CSV missing required columns: {missing}")

        df = df[REQUIRED_COLS].copy()

        # dtype cleanup
        int_cols = ["game_id", "player_id", "team_id", "cf", "ca", "toi_sec"]
        for c in int_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        float_cols = ["cf60", "ca60", "cf_percent"]
        for c in float_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["game_id", "player_id", "team_id", "cf", "ca", "toi_sec"])
        df = df.drop_duplicates(subset=["game_id", "player_id", "team_id"]).reset_index(drop=True)

        insert_data(df, table, session)

engine.dispose()
print("Done.")
