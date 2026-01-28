"""
Fill this in later.

Eric Winiecke
January 14, 2026.
"""

import pandas as pd
from sqlalchemy.orm import sessionmaker

from .data_processing_utils import insert_data
from .db_utils import create_player_game_es_table, get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

SEASONS = [20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025]

with Session() as session:
    for season in SEASONS:
        table_name = f"player_game_es_{season}"
        table = create_player_game_es_table(table_name, metadata)
        metadata.create_all(engine)

        df = pd.read_csv(file_path)

        # Normalize columns (Spotrac sometimes has weird spacing)
        df.columns = [c.strip() for c in df.columns]

        # Keep only what we need
        df = df[["firstName", "lastName", "capHit", "spotrac_url"]].copy()

        # Clean capHit if it came in as string (should already be numeric in your scrape)
        df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")

        # Drop rows missing required fields
        df = df.dropna(subset=["firstName", "lastName", "capHit"])

        # ✅ De-dupe by spotrac_url (best unique key you have)
        df = df.drop_duplicates(subset=["spotrac_url"], keep="first").reset_index(drop=True)

        # enforce schema columns only
        df = df[
            ["game_id", "player_id", "team_id", "cf", "ca", "toi_sec", "cf60", "ca60", "cf_percent"]
        ].copy()

        insert_data(df, table, session)

engine.dispose()
