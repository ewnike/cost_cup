"""
Build mart.player_game_es_{season} for modern seasons (20182019+).

Definition:
- Even-strength (ES) only: exclude penalty-imbalance segments where teams are unequal AND
  at least one team has < 5 skaters.
- CF/CA computed from PBP events that occur during ES time.
- TOI computed from shifts with penalty-exclusion overlap removed.

Inputs:
- derived.game_plays_{season}_from_raw_pbp
- derived.raw_shifts_resolved
- dim.dim_team_code

Output:
- mart.player_game_es_{season} with:
  (game_id, player_id, team_id, cf, ca, toi_sec, cf60, ca60, cf_percent)

Author: Eric Winiecke (standardized build script)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text

from constants import SCHEMA, SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger
from schema_utils import fq
from strength_utils import (
    apply_exclude_to_plays,
    build_exclude_timeline_equal_strength,
    filter_goalies_modern,
)

logger = setup_logger()


def get_num_players(shift_df: pd.DataFrame) -> pd.DataFrame:
    shifts_melted = pd.melt(
        shift_df,
        id_vars=["game_id", "player_id"],
        value_vars=["shift_start", "shift_end"],
    ).sort_values("value", ignore_index=True)

    shifts_melted["change"] = 2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
    shifts_melted["num_players"] = shifts_melted["change"].cumsum()

    df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()
    return df_num_players[
        df_num_players["num_players"].shift() != df_num_players["num_players"]
    ].reset_index(drop=True)


def get_penalty_exclude_times(game_shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Exclude when teams are imbalanced AND at least one team has < 5 skaters.
    Returns rows keyed by 'time' (absolute game seconds) with boolean 'exclude'.
    """
    if game_shifts.empty:
        return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

    team_1 = game_shifts.iloc[0]["team_id"]
    shifts_1 = game_shifts[game_shifts["team_id"] == team_1]
    shifts_2 = game_shifts[game_shifts["team_id"] != team_1]

    df1 = get_num_players(shifts_1).rename(columns={"value": "time", "num_players": "team_1"})
    df2 = get_num_players(shifts_2).rename(columns={"value": "time", "num_players": "team_2"})

    df_exclude = pd.concat([df1, df2]).sort_values("time", ignore_index=True).ffill()

    # keep only time boundaries
    df_exclude = df_exclude[df_exclude["time"].shift(-1) != df_exclude["time"]].reset_index(
        drop=True
    )

    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = (diff & missing).astype(bool)
    return df_exclude


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

    for i in range(len(ex_start)):
        a = np.maximum(interval_start, ex_start[i])
        b = np.minimum(interval_end, ex_end[i])
        out += np.maximum(0, b - a)
    return out


def build_es_toi_for_game(gs_game: pd.DataFrame) -> pd.DataFrame:
    """Return toi_sec (ES) per (game_id, player_id, team_id) for one game."""
    # ✅ drop goalies if we can
    gs_game = filter_goalies_modern(gs_game)

    # ✅ shared exclude timeline (same rule as legacy)
    df_ex = build_exclude_timeline_equal_strength(gs_game)

    ex_s, ex_e = exclude_intervals(df_ex)  # your existing helper

    gs_game["shift_start"] = gs_game["shift_start"].astype(int)
    gs_game["shift_end"] = gs_game["shift_end"].astype(int)

    s = gs_game["shift_start"].to_numpy(np.int64)
    e = gs_game["shift_end"].to_numpy(np.int64)
    dur = np.maximum(0, e - s)

    ov = overlap_seconds(s, e, ex_s, ex_e)
    es = dur - ov
    es[es < 0] = 0

    tmp = gs_game[["game_id", "player_id", "team_id"]].copy()
    tmp["toi_sec"] = es
    return tmp.groupby(["game_id", "player_id", "team_id"], as_index=False)["toi_sec"].sum()


def update_corsi_counts(
    df_corsi: pd.DataFrame, event: pd.Series, game_shifts: pd.DataFrame
) -> None:
    """Mutates df_corsi cf/ca in-place for one event."""
    time = int(event["time"])
    team_for = int(event["team_id_for"])
    team_against = int(event["team_id_against"])

    on_ice = game_shifts[(game_shifts["shift_start"] <= time) & (game_shifts["shift_end"] >= time)]
    players_for = on_ice.loc[on_ice["team_id"] == team_for, "player_id"].to_numpy()
    players_against = on_ice.loc[on_ice["team_id"] == team_against, "player_id"].to_numpy()

    ev = event["event"]
    if ev in ("Shot", "Goal", "Missed Shot"):
        df_corsi.loc[df_corsi["player_id"].isin(players_for), "cf"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against), "ca"] += 1
    elif ev == "Blocked Shot":
        df_corsi.loc[df_corsi["player_id"].isin(players_for), "ca"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against), "cf"] += 1


def build_player_game_es_for_season(season: int) -> pd.DataFrame:
    """Compute ES player-game stats for a season and return a dataframe."""
    engine = get_db_engine()

    plays_view = fq("derived", f"game_plays_{season}_from_raw_pbp")
    shifts_resolved = fq("derived", "raw_shifts_resolved")
    dim_team_code = fq("dim", "dim_team_code")

    gp = pd.read_sql(f"SELECT * FROM {plays_view}", engine)

    gs = pd.read_sql(
        f"""
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
        WHERE rs.season = {int(season)}
        AND rs.session = 'R'
        AND rs.seconds_end > rs.seconds_start
        """,
        engine,
    )

    engine.dispose()

    out_rows: list[pd.DataFrame] = []

    for game_id, gp_game in gp.groupby("game_id", sort=False):
        gs_game = gs[gs["game_id"] == game_id]
        if gp_game.empty or gs_game.empty:
            continue

        # Ensure plays have numeric time and are corsi-relevant
        gp_game = gp_game.copy()
        gp_game["time"] = pd.to_numeric(gp_game["time"], errors="coerce").astype("Int64")
        gp_game = gp_game.dropna(subset=["time"])
        gp_game["time"] = gp_game["time"].astype("int64")
        gp_game = gp_game[
            gp_game["event"].isin(["Shot", "Goal", "Missed Shot", "Blocked Shot"])
        ].dropna(subset=["team_id_for", "team_id_against"])

        if gp_game.empty:
            continue

        # ✅ filter goalies once; use same gs_game for TOI + CF/CA
        gs_game = filter_goalies_modern(gs_game)

        # TOI ES
        toi_game = build_es_toi_for_game(gs_game)

        df_corsi = toi_game[["game_id", "player_id", "team_id"]].drop_duplicates().copy()
        df_corsi["cf"] = 0
        df_corsi["ca"] = 0

        df_ex = build_exclude_timeline_equal_strength(gs_game)
        gp_game_es = apply_exclude_to_plays(gp_game, df_ex)

        for _, ev in gp_game_es.iterrows():
            update_corsi_counts(df_corsi, ev, gs_game)

        merged = df_corsi.merge(toi_game, on=["game_id", "player_id", "team_id"], how="left")

        merged["cf60"] = np.where(
            merged["toi_sec"] > 0, merged["cf"] * 3600.0 / merged["toi_sec"], np.nan
        )
        merged["ca60"] = np.where(
            merged["toi_sec"] > 0, merged["ca"] * 3600.0 / merged["toi_sec"], np.nan
        )
        merged["cf_percent"] = np.where(
            (merged["cf"] + merged["ca"]) > 0,
            100.0 * merged["cf"] / (merged["cf"] + merged["ca"]),
            0.0,
        )

        out_rows.append(merged)

    if not out_rows:
        return pd.DataFrame()

    return pd.concat(out_rows, ignore_index=True)


# def main() -> None:
#     """Make it happen."""
#     engine = get_db_engine()

#     for season in SEASONS_MODERN:
#         df = build_player_game_es_for_season(season)
#         if df.empty:
#             print(f"⚠️ {season}: no rows produced")
#             continue

#         out_table = f"player_game_es_{season}"
#         df = df[
#             ["game_id", "player_id", "team_id", "cf", "ca", "toi_sec", "cf60", "ca60", "cf_percent"]
#         ].copy()

#         df.to_sql(out_table, engine, schema=SCHEMA["mart"], if_exists="replace", index=False)
#         print(f"✅ wrote {len(df)} rows -> {SCHEMA['mart']}.{out_table}")

#     engine.dispose()


def main() -> None:
    """Build mart.player_game_es_{season} for all modern seasons."""
    engine = get_db_engine()

    for season in SEASONS_MODERN:
        df = build_player_game_es_for_season(int(season))
        if df.empty:
            print(f"⚠️ {season}: no rows produced")
            continue

        out_table = f"player_game_es_{season}"
        schema = SCHEMA["mart"]

        df = df[
            ["game_id", "player_id", "team_id", "cf", "ca", "toi_sec", "cf60", "ca60", "cf_percent"]
        ].copy()

        # dtype cleanup (prevents surprises)
        df["game_id"] = df["game_id"].astype("int64")
        df["player_id"] = df["player_id"].astype("int64")
        df["team_id"] = df["team_id"].astype("int64")
        df["cf"] = df["cf"].astype("int64")
        df["ca"] = df["ca"].astype("int64")
        df["toi_sec"] = df["toi_sec"].astype("int64")

        # truncate then append (table must exist; you can create it via db_utils.create_player_game_es_table)
        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{out_table}"'))
        df.to_sql(out_table, engine, schema=schema, if_exists="append", index=False, method="multi")

        print(f"✅ wrote {len(df)} rows -> {schema}.{out_table}")

    engine.dispose()


if __name__ == "__main__":
    main()
