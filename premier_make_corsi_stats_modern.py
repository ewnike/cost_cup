"""
Modern Corsi Calculation Script (PBP + raw shifts).

Even-strength definition used here:
  - ALLOW equal skater counts (5v5, 4v4, 3v3 OT, etc.)
  - EXCLUDE imbalanced skater counts (6v5, 5v4, 4v3, etc.)

Goalies:
  - We count SKATERS only.
  - Raw shifts may include goalies; we filter them out BEFORE counting skaters.
  - If shifts don't have a goalie flag/position, we use a robust heuristic:
      drop any player whose total TOI in the game is extremely high (>= 2200s).
    (This catches goalies reliably in almost all cases.)

Author: Eric Winiecke
Date: Oct 30, 2024 (updated January 2026.)
"""

import os

import pandas as pd

from constants import SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger
from schema_utils import fq

# from strength_utils import get_num_players
from strength_utils import build_exclude_timeline_equal_strength

# from .strength_utils import (
#     apply_exclude_to_plays,
#     build_exclude_timeline_equal_strength,
#     filter_goalies_modern,
# )

LOG_FILE_PATH = "/Users/ericwiniecke/Documents/github/cost_cup/logs/data_processing.log"
logger = setup_logger(LOG_FILE_PATH)
logger.info("Logger configured successfully.")


# -------------------------
# Time helpers
# -------------------------
def _period_time_to_seconds(s: pd.Series) -> pd.Series:
    """Convert periodTime to seconds if it is in 'MM:SS' format; otherwise returns numeric."""
    if s.dtype == "O":
        # If already numeric strings, try direct numeric conversion first
        numeric = pd.to_numeric(s, errors="coerce")
        if numeric.notna().any():
            # Mixed types: fill parsed numeric where possible
            out = numeric
        else:
            out = pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")

        # Parse MM:SS where numeric failed
        mask = out.isna() & s.astype(str).str.contains(":")
        if mask.any():
            parts = s[mask].astype(str).str.split(":", expand=True)
            mins = pd.to_numeric(parts[0], errors="coerce")
            secs = pd.to_numeric(parts[1], errors="coerce")
            out.loc[mask] = (mins * 60 + secs).astype("Float64")

        return out.astype("Float64")

    return pd.to_numeric(s, errors="coerce").astype("Float64")


def add_cumulative_time_from_period(gp: pd.DataFrame) -> pd.DataFrame:
    """
    Add a cumulative 'time' column (seconds from game start).

    Periods 1-3: (period-1)*1200 + periodTime
    OT (period >= 4): 3600 + periodTime.
    """
    gp = gp.copy()

    if "time" in gp.columns:
        return gp

    if "period" not in gp.columns or "periodTime" not in gp.columns:
        raise KeyError("game_plays is missing required columns: period, periodTime")

    pt = _period_time_to_seconds(gp["periodTime"])
    per = pd.to_numeric(gp["period"], errors="coerce")

    # OT+ goes on top of 3600
    base = (per - 1) * 1200
    base = base.where(per <= 3, 3600)

    gp["time"] = (base + pt).astype("Int64")

    return gp


# -------------------------
# Skater counting helpers
# -------------------------
# def get_num_players(shift_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Compute number of skaters on ice at each time breakpoint.

#     shift_df must have: game_id, player_id, shift_start, shift_end
#     """
#     if shift_df.empty:
#         return pd.DataFrame(columns=["value", "num_players"])

#     shifts_melted = (
#         pd.melt(
#             shift_df,
#             id_vars=["game_id", "player_id"],
#             value_vars=["shift_start", "shift_end"],
#         )
#         .sort_values("value", ignore_index=True)
#         .copy()
#     )

#     shifts_melted["change"] = 2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
#     shifts_melted["num_players"] = shifts_melted["change"].cumsum()

#     df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()

#     return df_num_players[
#         df_num_players["num_players"].shift() != df_num_players["num_players"]
#     ].reset_index(drop=True)


def drop_probable_goalies(game_shifts: pd.DataFrame, *, toi_threshold: int = 2200) -> pd.DataFrame:
    """
    Remove goalie rows from shifts using TOI heuristic when no goalie flag exists.

    - Compute total TOI per (game_id, team_id, player_id).
    - Drop players with TOI >= toi_threshold seconds.

    2200s (~36:40) is high enough that skaters virtually never hit it in regulation.
    Goalies generally exceed it, even with some time pulled.
    """
    if game_shifts.empty:
        return game_shifts

    required = {"game_id", "team_id", "player_id", "shift_start", "shift_end"}
    missing = required - set(game_shifts.columns)
    if missing:
        raise KeyError(f"drop_probable_goalies missing required columns: {sorted(missing)}")

    gs = game_shifts.copy()
    gs["shift_dur"] = (gs["shift_end"] - gs["shift_start"]).clip(lower=0)

    toi = (
        gs.groupby(["game_id", "team_id", "player_id"], as_index=False)["shift_dur"]
        .sum()
        .rename(columns={"shift_dur": "toi"})
    )

    goalies = toi[toi["toi"] >= toi_threshold][["game_id", "player_id"]].copy()
    if goalies.empty:
        # nothing flagged; return original
        return gs.drop(columns=["shift_dur"], errors="ignore")

    before = len(gs)
    gs = gs.merge(goalies.assign(_goalie=1), on=["game_id", "player_id"], how="left")
    gs = gs[gs["_goalie"].isna()].drop(columns=["_goalie", "shift_dur"], errors="ignore")
    after = len(gs)

    logger.info(f"Dropped probable goalie shift rows: {before - after}")
    return gs


# def get_exclude_timeline(game_shifts: pd.DataFrame, *, log_rows: bool = False) -> pd.DataFrame:
#     """
#     Build exclude timeline based on SKATER counts per team.

#     Your chosen rule:
#       exclude = (team_1 != team_2) & (team_1 <= 6) & (team_2 <= 6)

#     Allows 5v5, 4v4, 3v3 OT (equal) and excludes 6v5, 5v4, 4v3, etc.
#     """
#     if game_shifts.empty:
#         return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

#     gs = game_shifts.copy()

#     # Filter goalies BEFORE counting skaters
#     gs = drop_probable_goalies(gs)

# team_ids = sorted(gs["team_id"].dropna().unique())
# if len(team_ids) != 2:
#     logger.warning(f"Expected 2 teams in shifts, found {len(team_ids)}: {team_ids}")
#     return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

# t1, t2 = team_ids
# s1 = gs[gs["team_id"] == t1]
# s2 = gs[gs["team_id"] == t2]

# df1 = get_num_players(s1).rename(columns={"value": "time", "num_players": "team_1"})
# df2 = get_num_players(s2).rename(columns={"value": "time", "num_players": "team_2"})

# df_ex = (
#     pd.concat([df1, df2], ignore_index=True)
#     .sort_values("time", ignore_index=True)
#     .ffill()
#     .bfill()
# )

# # breakpoint rows only
# df_ex = df_ex[df_ex["time"].shift(-1) != df_ex["time"]].reset_index(drop=True)

# df_ex["exclude"] = (
#     (df_ex["team_1"] != df_ex["team_2"]) & (df_ex["team_1"] <= 6) & (df_ex["team_2"] <= 6)
# )
# df_ex = build_exclude_timeline_equal_strength(gs)

# if log_rows:
#     logger.info("Exclude timeline (first 50 rows):")
#     logger.info(df_ex.head(50))


# return df_ex
# def get_exclude_timeline(game_shifts: pd.DataFrame, *, log_rows: bool = False) -> pd.DataFrame:
#     """
#     Build exclude timeline based on SKATER counts per team.

#     Your chosen rule:
#       exclude = (team_1 != team_2) & (team_1 <= 6) & (team_2 <= 6)

#     Allows 5v5, 4v4, 3v3 OT (equal) and excludes 6v5, 5v4, 4v3, etc.
#     """
#     if game_shifts.empty:
#         return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

#     gs = game_shifts.copy()

#     # Filter goalies BEFORE counting skaters
#     # gs = drop_probable_goalies(gs)

#     # Delegate the core timeline build to the shared utility
#     df_ex = build_exclude_timeline_equal_strength(gs)

#     if df_ex.empty:
#         return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

#     if log_rows:
#         logger.info("Exclude timeline (first 50 rows):")
#         logger.info(df_ex.head(50))


#     return df_ex
# def get_exclude_timeline(
#     game_shifts_skaters: pd.DataFrame, *, log_rows: bool = False
# ) -> pd.DataFrame:
#     """
#     Build exclude timeline based on SKATER counts per team.

#     Assumes goalies have already been removed from game_shifts_skaters.
#     """
#     if game_shifts_skaters.empty:
#         return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

#     df_ex = build_exclude_timeline_equal_strength(game_shifts_skaters)

#     if log_rows and not df_ex.empty:
#         logger.info("Exclude timeline (first 50 rows):")
#         logger.info(df_ex.head(50))

#     return df_ex


def get_exclude_timeline(
    game_shifts_skaters: pd.DataFrame, *, log_rows: bool = False
) -> pd.DataFrame:
    """
    Build exclude timeline based on SKATER counts per team.

    Assumes goalies have already been removed.
    """
    if game_shifts_skaters.empty:
        return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

    df_ex = build_exclude_timeline_equal_strength(game_shifts_skaters)

    if log_rows and not df_ex.empty:
        logger.info("Exclude timeline (first 50 rows):")
        logger.info(df_ex.head(50))

    return df_ex


# -------------------------
# Corsi calculation helpers
# -------------------------
def prepare_game_plays(df_game: dict, relevant_events: list[str]) -> pd.DataFrame | None:
    """
    Prepare_game_plays.

    :param df_game: Description
    :type df_game: dict
    :param relevant_events: Description
    :type relevant_events: list[str]
    :return: Description
    :rtype: DataFrame | None
    """
    if "game_plays" not in df_game:
        logger.error("'game_plays' missing in df_game")
        return None

    gp = df_game["game_plays"].copy()
    gp = add_cumulative_time_from_period(gp)

    if "event" not in gp.columns:
        logger.error("'event' column missing in game_plays")
        return None

    needed = {"team_id_for", "team_id_against"}
    missing = needed - set(gp.columns)
    if missing:
        logger.error(f"Missing required team columns in game_plays: {sorted(missing)}")
        return None

    gp = (
        gp[gp["event"].isin(relevant_events)]
        .dropna(subset=["team_id_for", "team_id_against"])
        .copy()
    )

    # ensure int time
    gp["time"] = pd.to_numeric(gp["time"], errors="coerce").astype("Int64")
    gp = gp.dropna(subset=["time"]).copy()

    return gp


def update_corsi(
    df_corsi: pd.DataFrame, event: pd.Series, game_shifts: pd.DataFrame
) -> pd.DataFrame:
    """
    Update_corsi.

    :param df_corsi: Description
    :type df_corsi: pd.DataFrame
    :param event: Description
    :type event: pd.Series
    :param game_shifts: Description
    :type game_shifts: pd.DataFrame
    :return: Description
    :rtype: DataFrame
    """
    t = int(event["time"])
    team_for = int(event["team_id_for"])
    team_against = int(event["team_id_against"])

    players_on_ice = game_shifts[
        (game_shifts["shift_start"] <= t) & (game_shifts["shift_end"] >= t)
    ]

    players_for = players_on_ice[players_on_ice["team_id"] == team_for]
    players_against = players_on_ice[players_on_ice["team_id"] == team_against]

    if event["event"] in ["Shot", "Goal", "Missed Shot"]:
        df_corsi.loc[df_corsi["player_id"].isin(players_for["player_id"]), "corsi_for"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against["player_id"]), "corsi_against"] += 1
    elif event["event"] == "Blocked Shot":
        df_corsi.loc[df_corsi["player_id"].isin(players_for["player_id"]), "corsi_against"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against["player_id"]), "corsi_for"] += 1

    return df_corsi


def calculate_corsi_for_game(df_corsi: pd.DataFrame, df_game: dict) -> pd.DataFrame:
    """
    Calculate_corsi_for_game.

    :param df_corsi: Description
    :type df_corsi: pd.DataFrame
    :param df_game: Description
    :type df_game: dict
    :return: Description
    :rtype: DataFrame
    """
    gs = df_game["game_shifts"].copy()
    gp = df_game["game_plays"].copy()

    if gs.empty or gp.empty:
        return df_corsi

    # Ensure ints
    gs["shift_start"] = pd.to_numeric(gs["shift_start"], errors="coerce").astype("Int64")
    gs["shift_end"] = pd.to_numeric(gs["shift_end"], errors="coerce").astype("Int64")
    gs = gs.dropna(subset=["shift_start", "shift_end", "team_id", "player_id"]).copy()
    gs["shift_start"] = gs["shift_start"].astype(int)
    gs["shift_end"] = gs["shift_end"].astype(int)
    gs["team_id"] = gs["team_id"].astype(int)
    gs["player_id"] = gs["player_id"].astype(int)

    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
    plays = prepare_game_plays({"game_plays": gp}, relevant_events)
    if plays is None or plays.empty:
        return df_corsi

    # # Build exclude timeline (skater imbalance)
    # gs = drop_probable_goalies(gs)
    # df_ex = get_exclude_timeline(gs)

    # if df_ex.empty:
    #     # no exclude timeline; proceed without exclusions
    #     for _, event in plays.iterrows():
    #         df_corsi = update_corsi(df_corsi, event, gs)
    #     return df_corsi

    # if not df_ex.empty:
    #     assert df_ex["time"].is_monotonic_increasing
    #     assert set(df_ex.columns) >= {"time", "team_1", "team_2", "exclude"}

    # df_ex["time"] = df_ex["time"].astype(int)
    # plays["time"] = plays["time"].astype(int)

    # # Map play times to breakpoint index
    # idx = df_ex["time"].searchsorted(plays["time"]) - 1
    # idx[idx < 0] = 0
    # idx = idx.clip(0, len(df_ex) - 1)

    # mask = df_ex["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
    # plays = plays.loc[~mask]

    # for _, event in plays.iterrows():
    #     df_corsi = update_corsi(df_corsi, event, gs)

    # return df_corsi
    # Build exclude timeline (skater imbalance)
    gs = drop_probable_goalies(gs)
    df_ex = get_exclude_timeline(gs)

    if df_ex.empty:
        # no exclude timeline; proceed without exclusions
        for _, event in plays.iterrows():
            df_corsi = update_corsi(df_corsi, event, gs)
        return df_corsi

    # df_ex is non-empty here
    assert df_ex["time"].is_monotonic_increasing
    assert {"time", "team_1", "team_2", "exclude"}.issubset(df_ex.columns)

    df_ex["time"] = df_ex["time"].astype(int)
    plays = plays.copy()
    plays["time"] = plays["time"].astype(int)

    # Map play times to breakpoint index
    idx = df_ex["time"].searchsorted(plays["time"]) - 1
    idx[idx < 0] = 0
    idx = idx.clip(0, len(df_ex) - 1)

    mask = df_ex["exclude"].iloc[idx].to_numpy()
    plays = plays.loc[~mask]

    for _, event in plays.iterrows():
        df_corsi = update_corsi(df_corsi, event, gs)

    return df_corsi


def create_corsi_stats(df_corsi: pd.DataFrame, df_game: dict) -> pd.DataFrame:
    """
    Create_corsi_stats.

    :param df_corsi: Description
    :type df_corsi: pd.DataFrame
    :param df_game: Description
    :type df_game: dict
    :return: Description
    :rtype: DataFrame
    """
    df_corsi = df_corsi.copy()
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0

    df_corsi = calculate_corsi_for_game(df_corsi, df_game)

    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
    denom = df_corsi["corsi_for"] + df_corsi["corsi_against"]
    df_corsi["CF_Percent"] = ((df_corsi["corsi_for"] / denom) * 100).fillna(0).round(4)

    return df_corsi


# -------------------------
# Season driver
# -------------------------
def calculate_and_save_corsi_stats(season: int) -> None:
    """
    Calculate_and_save_corsi_stats.

    :param season: Description
    :type season: int
    """
    df_master = {}
    engine = get_db_engine()

    try:
        plays_view = fq("derived", f"game_plays_{season}_from_raw_pbp")
        df_master["game_plays"] = pd.read_sql(f"SELECT * FROM {plays_view}", engine)

        raw_shifts_resolved = fq("raw", "raw_shifts_resolved")
        dim_team_code = fq("dim", "dim_team_code")

        # IMPORTANT: this query builds shift_start/shift_end in cumulative seconds,
        # matching add_cumulative_time_from_period() for plays.
        df_master["game_shifts"] = pd.read_sql(
            f"""
            SELECT
                rs.game_id,
                rs.player_id_resolved AS player_id,
                dt.team_id,
                rs.game_period AS period,
                CASE
                    WHEN rs.game_period IN (1,2,3) THEN (rs.game_period - 1) * 1200 + rs.seconds_start
                    ELSE 3600 + rs.seconds_start
                END AS shift_start,
                CASE
                    WHEN rs.game_period IN (1,2,3) THEN (rs.game_period - 1) * 1200 + rs.seconds_end
                    ELSE 3600 + rs.seconds_end
                END AS shift_end
            FROM {raw_shifts_resolved} rs
            JOIN {dim_team_code} dt
                ON dt.team_code = rs.team
            WHERE rs.season = {int(season)}
              AND rs.session = 'R'
              AND rs.seconds_end > rs.seconds_start
            """,
            engine,
        )

    finally:
        engine.dispose()

    # basic validation
    if df_master["game_plays"].empty or df_master["game_shifts"].empty:
        logger.warning(f"{season}: missing plays/shifts data (empty).")
        return

    # Use game_ids present in plays view
    season_game_ids = sorted(df_master["game_plays"]["game_id"].dropna().astype(int).unique())
    logger.info(f"{season}: processing {len(season_game_ids)} games")

    plays_by_game = dict(tuple(df_master["game_plays"].groupby("game_id", sort=False)))
    shifts_by_game = dict(tuple(df_master["game_shifts"].groupby("game_id", sort=False)))

    season_out = []

    for game_id in season_game_ids:
        gp = plays_by_game.get(game_id)
        gs = shifts_by_game.get(game_id)

        if gp is None or gs is None or gp.empty or gs.empty:
            logger.warning(f"{season} game_id {game_id}: missing plays or shifts.")
            continue

        df_game = {"game_plays": gp, "game_shifts": gs}

        # Seed players from shifts (has team_id)
        df_corsi = gs[["game_id", "player_id", "team_id"]].drop_duplicates().copy()

        corsi_stats = create_corsi_stats(df_corsi, df_game)
        if not corsi_stats.empty:
            season_out.append(corsi_stats)

    if not season_out:
        logger.warning(f"{season}: no corsi output generated.")
        return

    final_df = pd.concat(season_out, ignore_index=True)

    out_dir = os.path.join(os.getcwd(), "corsi_stats")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"corsi_stats_{season}.csv")
    final_df.to_csv(out_file, index=False)
    logger.info(f"{season}: saved to {out_file}")


if __name__ == "__main__":
    for season in SEASONS_MODERN:
        logger.info(f"Running season {season}")
        calculate_and_save_corsi_stats(int(season))
