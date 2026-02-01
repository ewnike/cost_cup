"""
Build modern player-game stats (CSV-first) for seasons 20182019+.

Phase 1 (available now):
- Total TOI from shifts
- Even-strength TOI (exclude imbalanced skater time)
- ES CF/CA + rates (same logic as build_player_game_es.py)

Inputs:
- derived.game_plays_{season}_from_raw_pbp (team-level events, has time)
- derived.raw_shifts_resolved (resolved shifts)
- dim.dim_team_code

Output:
- player_game_stats/player_game_stats_{season}.csv
"""

from __future__ import annotations

import os
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import text

from constants import SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger
from schema_utils import fq

# Reuse from your modern ES builder
from strength_utils import (
    apply_exclude_to_plays,
    build_exclude_timeline_equal_strength,
    filter_goalies_modern,
)

logger = setup_logger()
OUT_DIR = "player_game_stats"


# -------------------------
# TOI helpers
# -------------------------
def build_total_toi_for_game(gs_game: pd.DataFrame) -> pd.DataFrame:
    """Total TOI (seconds) per (game_id, player_id, team_id) from shifts."""
    gs = gs_game.copy()
    gs["shift_start"] = gs["shift_start"].astype(int)
    gs["shift_end"] = gs["shift_end"].astype(int)
    dur = (gs["shift_end"] - gs["shift_start"]).clip(lower=0)

    out = gs[["game_id", "player_id", "team_id"]].copy()
    out["toi_total_sec"] = dur.to_numpy(np.int64)
    return out.groupby(["game_id", "player_id", "team_id"], as_index=False)["toi_total_sec"].sum()


def exclude_intervals(df_exclude: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Convert df_exclude rows into [start,end) intervals where exclude=True."""
    if df_exclude.empty:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64)

    t = df_exclude["time"].to_numpy(dtype=np.int64)
    # end boundary is next time; last row end doesn't matter much
    t_next = np.r_[t[1:], t[-1]]
    mask = df_exclude["exclude"].to_numpy(bool)

    starts = t[mask]
    ends = t_next[mask]
    good = ends > starts
    return starts[good], ends[good]


def overlap_seconds(
    interval_start: np.ndarray, interval_end: np.ndarray, ex_start: np.ndarray, ex_end: np.ndarray
) -> np.ndarray:
    """Overlap between many [interval_start, interval_end) and exclude intervals."""
    out = np.zeros(len(interval_start), dtype=np.int64)
    if len(ex_start) == 0:
        return out

    # simple loop over exclude intervals (usually not huge)
    for i in range(len(ex_start)):
        a = np.maximum(interval_start, ex_start[i])
        b = np.minimum(interval_end, ex_end[i])
        out += np.maximum(0, b - a)
    return out


def build_es_toi_for_game(gs_game: pd.DataFrame) -> pd.DataFrame:
    """Return toi_es_sec per (game_id, player_id, team_id) for one game."""
    gs_game = filter_goalies_modern(gs_game)

    df_ex = build_exclude_timeline_equal_strength(gs_game)
    ex_s, ex_e = exclude_intervals(df_ex)

    gs = gs_game.copy()
    gs["shift_start"] = gs["shift_start"].astype(int)
    gs["shift_end"] = gs["shift_end"].astype(int)

    s = gs["shift_start"].to_numpy(np.int64)
    e = gs["shift_end"].to_numpy(np.int64)
    dur = np.maximum(0, e - s)

    ov = overlap_seconds(s, e, ex_s, ex_e)
    es = dur - ov
    es[es < 0] = 0

    out = gs[["game_id", "player_id", "team_id"]].copy()
    out["toi_es_sec"] = es
    return out.groupby(["game_id", "player_id", "team_id"], as_index=False)["toi_es_sec"].sum()


# -------------------------
# ES CF/CA counting (same idea as build_player_game_es)
# -------------------------
def update_corsi_counts(df_corsi: pd.DataFrame, event: pd.Series, gs_game: pd.DataFrame) -> None:
    """Mutates df_corsi cf/ca in-place for one event."""
    t = int(event["time"])
    team_for = int(event["team_id_for"])
    team_against = int(event["team_id_against"])

    on_ice = gs_game[(gs_game["shift_start"] <= t) & (gs_game["shift_end"] >= t)]
    players_for = on_ice.loc[on_ice["team_id"] == team_for, "player_id"].to_numpy()
    players_against = on_ice.loc[on_ice["team_id"] == team_against, "player_id"].to_numpy()

    ev = event["event"]
    if ev in ("Shot", "Goal", "Missed Shot"):
        df_corsi.loc[df_corsi["player_id"].isin(players_for), "cf"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against), "ca"] += 1
    elif ev == "Blocked Shot":
        df_corsi.loc[df_corsi["player_id"].isin(players_for), "ca"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against), "cf"] += 1


def build_player_game_stats_for_season(
    season: int, *, limit_games: int | None = None
) -> pd.DataFrame:
    engine = get_db_engine()

    plays_view = fq("derived", f"game_plays_{season}_from_raw_pbp")
    shifts_resolved = fq("derived", "raw_shifts_resolved")
    dim_team_code = fq("dim", "dim_team_code")

    try:
        with engine.connect() as conn:
            gp = pd.read_sql_query(text(f"SELECT * FROM {plays_view}"), conn)

            gs = pd.read_sql_query(
                text(f"""
                    SELECT
                        rs.game_id,
                        rs.player_id_resolved AS player_id,
                        dt.team_id,
                        rs.position,
                        rs.game_period AS period,
                        CASE WHEN rs.game_period IN (1,2,3)
                            THEN (rs.game_period - 1) * 1200 + rs.seconds_start
                            ELSE 3600 + rs.seconds_start END AS shift_start,
                        CASE WHEN rs.game_period IN (1,2,3)
                            THEN (rs.game_period - 1) * 1200 + rs.seconds_end
                            ELSE 3600 + rs.seconds_end END AS shift_end
                    FROM {shifts_resolved} rs
                    JOIN {dim_team_code} dt
                      ON dt.team_code = rs.team
                    WHERE rs.season = :season
                      AND rs.session = 'R'
                      AND rs.seconds_end > rs.seconds_start
                """),
                conn,
                params={"season": int(season)},
            )
    finally:
        engine.dispose()

    if gp.empty or gs.empty:
        logger.warning("%s: missing plays/shifts data", season)
        return pd.DataFrame()

    # --- CLEAN SHIFTS DATA ---
    gs = gs.copy()

    # coerce critical IDs and times
    for col in ["game_id", "player_id", "team_id", "shift_start", "shift_end"]:
        gs[col] = pd.to_numeric(gs[col], errors="coerce")

    # drop rows missing critical fields
    gs = gs.dropna(subset=["game_id", "player_id", "team_id", "shift_start", "shift_end"]).copy()

    # cast after dropna
    gs["game_id"] = gs["game_id"].astype("int64")
    gs["player_id"] = gs["player_id"].astype("int64")
    gs["team_id"] = gs["team_id"].astype("int64")
    gs["shift_start"] = gs["shift_start"].astype("int64")
    gs["shift_end"] = gs["shift_end"].astype("int64")

    # sanity: enforce positive durations
    gs = gs[gs["shift_end"] > gs["shift_start"]].copy()

    # ensure gp time numeric
    gp = gp.copy()
    gp["time"] = pd.to_numeric(gp["time"], errors="coerce").astype("Int64")
    gp = gp.dropna(subset=["time"]).copy()
    gp["time"] = gp["time"].astype("int64")

    # ✅ ensure game_id numeric (needed for grouping/filtering)
    gp["game_id"] = pd.to_numeric(gp["game_id"], errors="coerce")
    gp = gp.dropna(subset=["game_id"]).copy()
    gp["game_id"] = gp["game_id"].astype("int64")

    # corsi-relevant events only (for phase 1)
    gp = gp[gp["event"].isin(["Shot", "Goal", "Missed Shot", "Blocked Shot"])].copy()

    gp["team_id_for"] = pd.to_numeric(gp["team_id_for"], errors="coerce")
    gp["team_id_against"] = pd.to_numeric(gp["team_id_against"], errors="coerce")
    gp = gp.dropna(subset=["team_id_for", "team_id_against"]).copy()

    gp["team_id_for"] = gp["team_id_for"].astype("int64")
    gp["team_id_against"] = gp["team_id_against"].astype("int64")

    gp = gp.sort_values(["game_id", "time"], ignore_index=True)

    # limit games for test runs
    gp_ids = set(gp["game_id"].unique())
    gs_ids = set(gs["game_id"].unique())
    game_ids = sorted(gp_ids & gs_ids)

    if limit_games is not None:
        game_ids = game_ids[:limit_games]
        gp = gp[gp["game_id"].isin(game_ids)].copy()
        gs = gs[gs["game_id"].isin(game_ids)].copy()

    out_rows: List[pd.DataFrame] = []

    plays_by_game = dict(tuple(gp.groupby("game_id", sort=False)))
    shifts_by_game = dict(tuple(gs.groupby("game_id", sort=False)))

    for game_id in game_ids:
        gp_game = plays_by_game.get(game_id)
        gs_game = shifts_by_game.get(game_id)
        if gp_game is None or gs_game is None or gp_game.empty or gs_game.empty:
            continue

        # skaters only
        gs_skaters = filter_goalies_modern(gs_game)

        # TOI
        toi_total = build_total_toi_for_game(gs_skaters)
        toi_es = build_es_toi_for_game(gs_skaters)

        # ES plays (exclude imbalanced segments)
        df_ex = build_exclude_timeline_equal_strength(gs_skaters)
        gp_es = apply_exclude_to_plays(gp_game, df_ex)

        # CF/CA
        df_corsi = toi_es[["game_id", "player_id", "team_id"]].drop_duplicates().copy()
        df_corsi["cf"] = 0
        df_corsi["ca"] = 0

        for _, ev in gp_es.iterrows():
            update_corsi_counts(df_corsi, ev, gs_skaters)

        merged = df_corsi.merge(
            toi_total, on=["game_id", "player_id", "team_id"], how="left"
        ).merge(toi_es, on=["game_id", "player_id", "team_id"], how="left")

        merged["cf60"] = np.where(
            merged["toi_es_sec"] > 0, merged["cf"] * 3600.0 / merged["toi_es_sec"], np.nan
        )
        merged["ca60"] = np.where(
            merged["toi_es_sec"] > 0, merged["ca"] * 3600.0 / merged["toi_es_sec"], np.nan
        )
        merged["cf_percent"] = np.where(
            (merged["cf"] + merged["ca"]) > 0,
            100.0 * merged["cf"] / (merged["cf"] + merged["ca"]),
            0.0,
        )

        merged["season"] = str(season)
        out_rows.append(merged)

    if not out_rows:
        return pd.DataFrame()

    out = pd.concat(out_rows, ignore_index=True)

    # final dtype cleanup
    out["game_id"] = out["game_id"].astype("int64")
    out["player_id"] = out["player_id"].astype("int64")
    out["team_id"] = out["team_id"].astype("int64")
    out["cf"] = out["cf"].astype("int64")
    out["ca"] = out["ca"].astype("int64")
    out["toi_total_sec"] = out["toi_total_sec"].astype("int64")
    out["toi_es_sec"] = out["toi_es_sec"].astype("int64")

    return out


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    # Start with a small test:
    # set limit_games=3 for a smoke test, then remove it.

    TEST_MODE = False
    TEST_LIMIT_GAMES = 3 if TEST_MODE else None

    for season in SEASONS_MODERN:
        logger.info("Building player-game stats for %s", season)
        df = build_player_game_stats_for_season(int(season), limit_games=TEST_LIMIT_GAMES)
        suffix = "_test" if TEST_MODE else ""
        if df.empty:
            logger.warning("%s: no rows produced", season)
            continue

        out_file = os.path.join(OUT_DIR, f"player_game_stats_{season}{suffix}.csv")
        df.to_csv(out_file, index=False)
        logger.info("%s: wrote %s rows -> %s", season, len(df), out_file)


if __name__ == "__main__":
    main()
