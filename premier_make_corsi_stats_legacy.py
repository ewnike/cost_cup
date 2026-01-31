"""
Corsi Calculation and Debugging Script (LEGACY).

Legacy seasons:
  SEASONS_LEGACY = [20152016, 20162017, 20172018]

Definition used for "even strength" in this script:
  - Allow equal skater counts (5v5, 4v4, 3v3 OT, etc.)
  - Exclude imbalanced skater counts (5v4, 6v5, 4v3, etc.)

Skater counts are computed from game_shifts AFTER filtering out goalies.
Goalies are filtered by merging team_id from game_skater_stats (skaters-only)
and dropping rows with null team_id (goalies won't match and will be null).

Author: Eric Winiecke
Date: October 30, 2024 (updated)
"""

import os

import pandas as pd

from constants import SEASONS_LEGACY, TABLES
from load_data import get_env_vars, load_data
from log_utils import setup_logger
from schema_utils import fq
from strength_utils import (
    apply_exclude_to_plays,
    build_exclude_timeline_equal_strength,
    ensure_team_id_on_shifts_legacy,
    get_num_players,
)

GAME_TBL = fq(*TABLES["game"])


LOG_FILE_PATH = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing.log"
logger = setup_logger(LOG_FILE_PATH)
logger.info("Logger configured successfully. Test message to ensure logging works.")


# def get_num_players(shift_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Compute number of players (skaters) on ice at each time breakpoint.

#     shift_df columns required: ['game_id', 'player_id', 'shift_start', 'shift_end']
#     Returns: DataFrame with columns ['value' (time), 'num_players'].
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

#     # shift_start => +1, shift_end => -1
#     shifts_melted["change"] = 2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
#     shifts_melted["num_players"] = shifts_melted["change"].cumsum()

#     df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()

#     # Keep only rows where the count changes
#     df_num_players = df_num_players[
#         df_num_players["num_players"].shift() != df_num_players["num_players"]
#     ].reset_index(drop=True)

#     return df_num_players


def _ensure_team_id_on_shifts(
    game_shifts: pd.DataFrame,
    game_skater_stats: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ensure game_shifts has a 'team_id' column for skaters, and drop goalie rows.

    Legacy: game_shifts has no team_id. We merge from game_skater_stats (skaters-only).
    Goalies won't match -> team_id null -> dropped.

    Returns:
      game_shifts with team_id, and goalies removed.

    """
    if game_shifts.empty:
        return game_shifts

    if game_skater_stats is None or game_skater_stats.empty:
        raise ValueError("game_skater_stats is required to attach team_id and filter goalies.")

    merged = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left",
        suffixes=("_shift", "_gss"),
    )

    # If shifts already had team_id, prefer it; otherwise use gss.
    if "team_id_shift" in merged.columns and "team_id_gss" in merged.columns:
        merged["team_id"] = merged["team_id_shift"].combine_first(merged["team_id_gss"])
        merged = merged.drop(columns=["team_id_shift", "team_id_gss"], errors="ignore")
    elif "team_id_gss" in merged.columns and "team_id" not in merged.columns:
        merged = merged.rename(columns={"team_id_gss": "team_id"})
    elif "team_id" not in merged.columns:
        raise ValueError("Unable to construct 'team_id' on game_shifts.")

    # ✅ filter out goalies/unmatched
    merged = merged.dropna(subset=["team_id"]).copy()
    merged["team_id"] = merged["team_id"].astype(int)

    return merged


def get_penalty_exclude_times(
    game_shifts: pd.DataFrame,
    game_skater_stats: pd.DataFrame,
    *,
    log_rows: bool = False,
) -> pd.DataFrame:
    """
    Build exclude timeline based on skater counts by team.

    ALLOW: equal-strength skater counts (5v5, 4v4, 3v3 OT, etc.)
    EXCLUDE: imbalanced skater counts (6v5, 5v4, 4v3, etc.)

    Returns columns: time, team_1, team_2, exclude
    """
    if game_shifts.empty:
        logger.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
        return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

    game_shifts = _ensure_team_id_on_shifts(game_shifts, game_skater_stats)

    team_ids = sorted(game_shifts["team_id"].unique())
    if len(team_ids) != 2:
        logger.warning(f"Expected 2 teams in game_shifts, found {len(team_ids)}: {team_ids}")
        return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

    t1, t2 = team_ids
    shifts_1 = game_shifts[game_shifts["team_id"] == t1]
    shifts_2 = game_shifts[game_shifts["team_id"] == t2]

    df_num_players_1 = get_num_players(shifts_1).rename(
        columns={"value": "time", "num_players": "team_1"}
    )
    df_num_players_2 = get_num_players(shifts_2).rename(
        columns={"value": "time", "num_players": "team_2"}
    )

    df_exclude = (
        pd.concat([df_num_players_1, df_num_players_2], ignore_index=True)
        .sort_values("time", ignore_index=True)
        .ffill()
        .bfill()
    )

    # Keep only breakpoint rows
    df_exclude = df_exclude[df_exclude["time"].shift(-1) != df_exclude["time"]].reset_index(
        drop=True
    )

    # ✅ Your agreed rule: exclude imbalance (includes 6v5, excludes 5v4, etc.; allows 4v4/3v3)
    df_exclude["exclude"] = (
        (df_exclude["team_1"] != df_exclude["team_2"])
        & (df_exclude["team_1"] <= 6)
        & (df_exclude["team_2"] <= 6)
    )

    if log_rows:
        logger.info("Exclude Timeline (skater imbalance):")
        for _, row in df_exclude.iterrows():
            logger.info(
                f"Time={row['time']} team_1={row['team_1']} team_2={row['team_2']} exclude={row['exclude']}"
            )

    return df_exclude


def prepare_game_plays(df: dict, relevant_events: list[str]) -> pd.DataFrame | None:
    """
    Prepare game_plays for Corsi events.

      - add cumulative time
      - filter to corsi-relevant events
      - require both team_id_for/team_id_against.
    """
    if "game_plays" not in df:
        logger.error("'game_plays' DataFrame missing in df.")
        return None

    gp = df["game_plays"].copy()

    if "periodTime" not in gp.columns or "period" not in gp.columns:
        logger.error("'periodTime' or 'period' column missing in game_plays.")
        return None

    gp.loc[:, "time"] = gp["periodTime"] + (gp["period"] - 1) * 1200

    gp = (
        gp[gp["event"].isin(relevant_events)]
        .dropna(subset=["team_id_for", "team_id_against"])
        .copy()
    )

    if gp.empty:
        return gp

    if "time" not in gp.columns:
        logger.error("The 'time' column is missing after filtering.")
        return None

    return gp


def update_corsi(
    df_corsi: pd.DataFrame, event: pd.Series, game_shifts: pd.DataFrame
) -> pd.DataFrame:
    """Update player-level Corsi on a single event."""
    time = event["time"]
    team_for = event["team_id_for"]
    team_against = event["team_id_against"]

    players_on_ice = game_shifts[
        (game_shifts["shift_start"] <= time) & (game_shifts["shift_end"] >= time)
    ]

    players_for = players_on_ice[players_on_ice["team_id"] == team_for]
    players_against = players_on_ice[players_on_ice["team_id"] == team_against]

    if event["event"] in ["Shot", "Goal", "Missed Shot"]:
        df_corsi.loc[df_corsi["player_id"].isin(players_for["player_id"]), "corsi_for"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against["player_id"]), "corsi_against"] += 1
    elif event["event"] == "Blocked Shot":
        # Blocked shot: shooter team gets CA, defending team gets CF
        df_corsi.loc[df_corsi["player_id"].isin(players_for["player_id"]), "corsi_against"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against["player_id"]), "corsi_for"] += 1

    return df_corsi


def calculate_corsi_for_game(
    df_corsi: pd.DataFrame,
    game_id: int,
    df: dict,
    game_plays: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate Corsi for a single game, excluding imbalanced skater-count time."""
    game_shifts = df["game_shifts"].query(f"game_id == {game_id}").copy()
    plays_game = game_plays.query(f"game_id == {game_id}").copy()

    if plays_game.empty or game_shifts.empty:
        return df_corsi

    if "time" not in plays_game.columns:
        logger.error("'time' column missing in plays_game after filtering by game_id.")
        return df_corsi

    gss = (
        df["game_skater_stats"]
        .query(f"game_id == {game_id}")[["game_id", "player_id", "team_id"]]
        .copy()
    )
    if gss.empty:
        return df_corsi

    # 1) Make shifts skaters-only + attach team_id
    before = len(game_shifts)
    game_shifts = ensure_team_id_on_shifts_legacy(game_shifts, gss)
    if game_shifts.empty:
        logger.warning("game_id %s: no skater shifts after merge/goalie filter", game_id)
        return df_corsi
    logger.info(
        "game_id %s: dropped %s shift rows (goalies/unmatched); remaining=%s",
        game_id,
        before - len(game_shifts),
        len(game_shifts),
    )

    # 2) Ensure int times
    game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
    game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

    # 3) Build exclude timeline (allow equal strength; exclude imbalanced incl 6v5)
    ex_timeline = build_exclude_timeline_equal_strength(game_shifts)

    # 4) Filter plays using exclude timeline
    plays_game = apply_exclude_to_plays(plays_game, ex_timeline)

    # 5) Update Corsi from remaining events
    for _, event in plays_game.iterrows():
        df_corsi = update_corsi(df_corsi, event, game_shifts)

    return df_corsi


def create_corsi_stats(df_corsi: pd.DataFrame, df: dict) -> pd.DataFrame:
    """Generate Corsi stats for all players across games included in df_corsi."""
    logger.info("Entered create_corsi_stats")

    df_corsi = df_corsi.copy()
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0

    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
    game_plays = prepare_game_plays(df, relevant_events)
    if game_plays is None or game_plays.empty:
        return df_corsi

    # Process each game once
    for game_id in df_corsi["game_id"].unique():
        df_corsi = calculate_corsi_for_game(df_corsi, game_id, df, game_plays)

    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
    denom = df_corsi["corsi_for"] + df_corsi["corsi_against"]
    df_corsi["CF_Percent"] = ((df_corsi["corsi_for"] / denom) * 100).fillna(0).round(4)

    logger.info("Corsi calculations complete. Summary:")
    logger.info(df_corsi.head(5))
    return df_corsi


def calculate_and_save_corsi_stats(season_game_ids, season):
    """Calculate Corsi for all games in a season and save to CSV."""
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    required = {"game_plays", "game_shifts", "game_skater_stats"}
    if not required.issubset(df_master.keys()):
        logger.error(f"Missing required dataframes. Have={list(df_master.keys())}")
        return

    season_corsi_stats = []

    for game_id in season_game_ids:
        df_game = {
            name: df[df["game_id"] == game_id]
            for name, df in df_master.items()
            if "game_id" in df.columns
        }

        if (
            df_game.get("game_shifts", pd.DataFrame()).empty
            or df_game.get("game_plays", pd.DataFrame()).empty
            or df_game.get("game_skater_stats", pd.DataFrame()).empty
        ):
            logger.warning(f"Skipping game {game_id}: missing shifts/plays/skater_stats.")
            continue

        df_corsi = df_game["game_skater_stats"][["game_id", "player_id", "team_id"]].copy()
        corsi_stats = create_corsi_stats(df_corsi, df_game)

        if corsi_stats is not None and not corsi_stats.empty:
            season_corsi_stats.append(corsi_stats)
            logger.info(f"Completed Corsi calculation for game {game_id}.")

    if season_corsi_stats:
        final_season_df = pd.concat(season_corsi_stats, ignore_index=True)
        output_dir = os.path.join(os.getcwd(), "corsi_stats")
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, f"corsi_stats_{season}.csv")
        final_season_df.to_csv(output_file, index=False)
        logger.info(f"Saved Corsi data for the {season} season to {output_file}.")
    else:
        logger.warning(f"No valid Corsi data was generated for the {season} season.")


if __name__ == "__main__":
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    if "game" not in df_master:
        logger.error("The 'game' DataFrame is missing from the loaded data. Cannot proceed.")
        raise SystemExit(1)

    for season in SEASONS_LEGACY:
        season_game_ids = (
            df_master["game"].loc[df_master["game"]["season"] == season, "game_id"].unique()
        )

        if len(season_game_ids) > 0:
            logger.info(f"Found {len(season_game_ids)} games for the {season} season.")
            calculate_and_save_corsi_stats(season_game_ids, season)
        else:
            logger.warning(f"No games found for the {season} season. Skipping.")
