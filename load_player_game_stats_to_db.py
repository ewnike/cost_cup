"""
Load modern player-game stats CSVs into AWS Postgres.

Writes one table per season:
  mart.player_game_stats_{season}

Assumes CSVs exist at:
  player_game_stats/player_game_stats_{season}.csv
"""

from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import text

from constants import SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger

logger = setup_logger()
OUT_DIR = "player_game_stats"
SCHEMA = "mart"


REQUIRED_COLS = [
    "season",
    "game_id",
    "player_id",
    "team_id",
    "cf",
    "ca",
    "toi_total_sec",
    "toi_es_sec",
    "cf60",
    "ca60",
    "cf_percent",
]


def load_one_season(engine, season: int) -> None:
    path = os.path.join(OUT_DIR, f"player_game_stats_{season}.csv")
    if not os.path.exists(path):
        logger.warning("Missing file: %s", path)
        return

    df = pd.read_csv(path)

    missing = set(REQUIRED_COLS) - set(df.columns)
    if missing:
        raise KeyError(f"{season}: CSV missing required columns: {sorted(missing)}")

    # dtype cleanup (prevents surprises + speeds inserts)
    df = df[REQUIRED_COLS].copy()
    df["season"] = df["season"].astype(str)
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")
    df["cf"] = pd.to_numeric(df["cf"], errors="coerce").astype("Int64")
    df["ca"] = pd.to_numeric(df["ca"], errors="coerce").astype("Int64")
    df["toi_total_sec"] = pd.to_numeric(df["toi_total_sec"], errors="coerce").astype("Int64")
    df["toi_es_sec"] = pd.to_numeric(df["toi_es_sec"], errors="coerce").astype("Int64")
    df["cf60"] = pd.to_numeric(df["cf60"], errors="coerce")
    df["ca60"] = pd.to_numeric(df["ca60"], errors="coerce")
    df["cf_percent"] = pd.to_numeric(df["cf_percent"], errors="coerce")

    # drop rows missing key fields
    df = df.dropna(subset=["game_id", "player_id", "team_id"]).copy()

    # cast to concrete types
    df["game_id"] = df["game_id"].astype("int64")
    df["player_id"] = df["player_id"].astype("int64")
    df["team_id"] = df["team_id"].astype("int64")
    df["cf"] = df["cf"].fillna(0).astype("int64")
    df["ca"] = df["ca"].fillna(0).astype("int64")
    df["toi_total_sec"] = df["toi_total_sec"].fillna(0).astype("int64")
    df["toi_es_sec"] = df["toi_es_sec"].fillna(0).astype("int64")

    table = f"player_game_stats_{season}"

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        # replace each season cleanly
        conn.execute(text(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"'))

    # write table
    df.to_sql(
        table,
        engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=50_000,
    )

    # add useful indexes (optional but recommended)
    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE INDEX IF NOT EXISTS "{table}_game_idx" ON "{SCHEMA}"."{table}" (game_id)')
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS "{table}_player_idx" ON "{SCHEMA}"."{table}" (player_id)'
            )
        )
        conn.execute(
            text(f'CREATE INDEX IF NOT EXISTS "{table}_team_idx" ON "{SCHEMA}"."{table}" (team_id)')
        )

    logger.info("%s: wrote %s rows -> %s.%s", season, len(df), SCHEMA, table)


def main() -> None:
    engine = get_db_engine()
    try:
        for season in SEASONS_MODERN:
            load_one_season(engine, int(season))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
