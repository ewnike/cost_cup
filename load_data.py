"""
load_data.py.

Load NHL data from Postgres into a dict of DataFrames.

Updated:
- Uses db_utils.get_db_engine() (single source of truth for DB connection)
- Reads from raw.* tables (raw.game, raw.game_plays, raw.game_shifts, raw.game_skater_stats)
- Optional season/game_id filtering to avoid loading everything
"""

from __future__ import annotations

import os
import pathlib
from typing import Any

import pandas as pd

from db_utils import get_db_engine, load_environment_variables
from schema_utils import fq

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")


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

    NOTE: Updated to use raw.game (not public.game_plays).
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
    Load core raw tables.

    If season is provided:
      - loads raw.game filtered to that season (optionally limit_games)
      - loads plays/shifts/skater_stats only for those game_ids

    If game_id is provided:
      - loads that single game across tables

    Returns dict with keys:
      game, game_plays, game_shifts, game_skater_stats
    """
    engine = get_db_engine()

    game_tbl = fq("raw", "game")
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

        # df_game = pd.read_sql(
        #     f"""
        #     SELECT game_id, season, type, date_time_GMT, away_team_id, home_team_id,
        #            away_goals, home_goals, outcome
        #     FROM {game_tbl}
        #     {where_sql}
        #     """,
        #     engine,
        #     params=params if params else None,
        # )
        df_game = pd.read_sql(
            f"SELECT * FROM {game_tbl} {where_sql}",
            engine,
            params=params if params else None,
        )

        if limit_games is not None and not df_game.empty:
            df_game = df_game.head(int(limit_games)).copy()

        # Determine game_ids scope for the other tables
        game_ids = df_game["game_id"].astype("int64").tolist() if not df_game.empty else None

        # def _read_by_game_ids(sql_base: str) -> pd.DataFrame:
        #     if game_ids is None:
        #         return pd.read_sql(sql_base, engine)
        #     return pd.read_sql(
        #         sql_base + " WHERE game_id = ANY(%(game_ids)s)",
        #         engine,
        #         params={"game_ids": game_ids},
        #     )
        def _read_by_game_ids(sql_base: str) -> pd.DataFrame:
            base = sql_base.strip().rstrip(";")
            if game_ids is None:
                return pd.read_sql(base, engine)
            return pd.read_sql(
                f"SELECT * FROM ({base}) q WHERE q.game_id = ANY(%(game_ids)s)",
                engine,
                params={"game_ids": game_ids},
            )

        # df_plays = _read_by_game_ids(
        #     f"""
        #     SELECT play_id, game_id, team_id_for, team_id_against, event, "secondaryType",
        #            x, y, period, "periodType", "periodTime", "periodTimeRemaining",
        #            "dateTime", goals_away, goals_home, description, st_x, st_y
        #     FROM {plays_tbl}
        #     """
        # )
        df_plays = _read_by_game_ids(f"SELECT * FROM {plays_tbl}")

        df_shifts = _read_by_game_ids(
            f"""
            SELECT game_id, player_id, period, shift_start, shift_end
            FROM {shifts_tbl}
            """
        )

        # df_gss = _read_by_game_ids(
        #     f"""
        #     SELECT game_id, player_id, team_id,
        #            "timeOnIce", assists, goals, shots, hits,
        #            "powerPlayGoals", "powerPlayAssists", "penaltyMinutes",
        #            "faceOffWins", "faceoffTaken", takeaways, giveaways,
        #            "shortHandedGoals", "shortHandedAssists", blocked, "plusMinus",
        #            "evenTimeOnIce", "shortHandedTimeOnIce", "powerPlayTimeOnIce"
        #     FROM {gss_tbl}
        #     """
        # )
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
    # Backwards compatible
    _ = get_env_vars()

    # Example debug: load one game
    df = load_data(game_id=2015020001, debug_print_head=True)

    print("Data loaded successfully.")
    for name, frame in df.items():
        print(f"{name}: {len(frame)} rows")
