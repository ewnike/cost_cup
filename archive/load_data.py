"""
load_data.py.

Load NHL data from Postgres into a dict of DataFrames.

Updated:
- Uses db_utils.get_db_engine() (single source of truth for DB connection)
- Reads from raw.* tables (raw.game, raw.game_plays, raw.game_shifts, raw.game_skater_stats)
- Optional season/game_id filtering to avoid loading everything
"""

raise SystemExit(
    "ARCHIVED/DEPRECATED: This script is no longer part of the active Cost of Cup pipeline.\n"
    "Do not run. Kept for historical reference only.\n\n"
    "Use the modern/golden pipeline (raw.raw_shifts_resolved + derived.game_plays_* + mart builds)."
)

from __future__ import annotations

import os
import pathlib
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from constants import SEASONS_MODERN
from db_utils import get_db_engine, load_environment_variables
from schema_utils import fq

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")


def _relation_exists(engine, fq_name: str) -> bool:
    """
    Return True if a fully-qualified relation exists (table/view/materialized view).

    fq_name should look like: derived.game_20182019_from_raw_pbp
    """
    schema, rel = fq_name.split(".", 1)
    q = text("SELECT to_regclass(:name) IS NOT NULL AS ok")
    with engine.connect() as conn:
        return bool(conn.execute(q, {"name": f'{schema}."{rel}"'}).scalar())


def get_env_vars() -> dict[str, Any]:
    """
    Kept for backwards compatibility with existing scripts.

    Your db_utils.get_db_engine() already loads env vars from .env.
    """
    load_environment_variables()
    return {}  # legacy scripts can still call get_env_vars(), but it isn't needed anymore


def fetch_game_ids_20152016(engine) -> list[int]:
    """
    Fetch all game IDs for 20152016 season.

    NOTE: raw.game is authoritative for game_id lists; legacy schema.* is deprecated.
    """
    game_tbl = fq("raw", "game")
    q = f"""
    SELECT DISTINCT game_id
    FROM {game_tbl}
    WHERE season = 20152016
    ORDER BY game_id;
    """
    return pd.read_sql(q, engine)["game_id"].astype("int64").tolist()


def load_data(
    env_vars: dict[str, Any] | None = None,
    *,
    season: int | None = None,
    game_id: int | None = None,
    limit_games: int | None = None,
    debug_print_head: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Load the core game-level tables into a dict of DataFrames, with season-aware routing.

    Sources:
      - Legacy seasons (and any season not in SEASONS_MODERN):
          game metadata comes from raw.game
      - Modern seasons (season in SEASONS_MODERN):
          game metadata comes from derived.game_{season}_from_raw_pbp
          (because raw.game is not reliably populated for modern seasons)

    Filtering behavior:
      - If `season` is provided:
          * Loads the "game" table filtered to that season.
          * Derives the list of game_ids from that filtered game set (optionally truncated
            by `limit_games`) and then loads plays/shifts/skater_stats for those game_ids.
      - If `game_id` is provided:
          * Loads that single game (and its associated plays/shifts/skater_stats).
      - If neither is provided:
          * Loads full tables (use with caution; can be large).

    Args:
        env_vars: Kept for backwards compatibility; ignored (db_utils.get_db_engine loads .env).
        season: Season identifier, e.g. 20152016 or 20182019.
        game_id: Single game_id to load.
        limit_games: If `season` is set, limit the number of games loaded (useful for debugging).
        debug_print_head: If True, prints row counts and the first few rows per table.

    Returns:
        A dict of DataFrames with keys:
          - "game"
          - "game_plays"
          - "game_shifts"
          - "game_skater_stats"

    """
    engine = get_db_engine()

    raw_game_tbl = fq("raw", "game")

    # choose the correct "game" source
    if season is not None and int(season) in SEASONS_MODERN:
        derived_game_tbl = fq("derived", f"game_{int(season)}_from_raw_pbp")

        # Prefer derived game view/table if it exists; otherwise fall back.
        game_tbl = derived_game_tbl if _relation_exists(engine, derived_game_tbl) else raw_game_tbl
    else:
        game_tbl = raw_game_tbl

    plays_tbl = fq("raw", "game_plays")
    shifts_tbl = fq("raw", "game_shifts")
    gss_tbl = fq("raw", "game_skater_stats")

    try:
        where = []
        params: dict[str, Any] = {}

        if season is not None:
            where.append("season = %(season)s")
            params["season"] = int(season)

        if game_id is not None:
            where.append("game_id = %(game_id)s")
            params["game_id"] = int(game_id)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        df_game = pd.read_sql(
            f"SELECT * FROM {game_tbl} {where_sql}",
            engine,
            params=params if params else None,
        )

        if limit_games is not None and not df_game.empty:
            df_game = df_game.head(int(limit_games)).copy()

        # Determine game_ids scope for the other tables
        game_ids = df_game["game_id"].astype("int64").tolist() if not df_game.empty else None

        def _read_by_game_ids(sql_base: str) -> pd.DataFrame:
            base = sql_base.strip().rstrip(";")
            if game_ids is None:
                return pd.read_sql(base, engine)
            return pd.read_sql(
                f"SELECT * FROM ({base}) q WHERE q.game_id = ANY(%(game_ids)s)",
                engine,
                params={"game_ids": game_ids},
            )

        df_plays = _read_by_game_ids(f"SELECT * FROM {plays_tbl}")

        df_shifts = _read_by_game_ids(
            f"""
            SELECT game_id, player_id, period, shift_start, shift_end
            FROM {shifts_tbl}
            """
        )

        df_gss = _read_by_game_ids(f"SELECT * FROM {gss_tbl}")

        # Normalize id dtypes for merges
        for name, df in {
            "game": df_game,
            "game_plays": df_plays,
            "game_shifts": df_shifts,
            "game_skater_stats": df_gss,
        }.items():
            if not df.empty and "game_id" in df.columns:
                df["game_id"] = df["game_id"].astype("int64")
            if not df.empty and "player_id" in df.columns:
                df["player_id"] = df["player_id"].astype("int64")

            if debug_print_head:
                print(f"{name}: {len(df)} rows")
                print(df.head(3))

        return {
            "game": df_game,
            "game_plays": df_plays,
            "game_shifts": df_shifts,
            "game_skater_stats": df_gss,
        }

    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(
        "DEPRECATED: Do not run load_data.py directly. It exists only for legacy imports.\n"
        "Prefer modern pipeline scripts and schema-qualified SQL."
    )
    # Backwards compatible
    _ = get_env_vars()

    # Example debug: load one game
    df = load_data(game_id=2015020001, debug_print_head=True)

    print("Data loaded successfully.")
    for name, frame in df.items():
        print(f"{name}: {len(frame)} rows")
