"""
Corsi Calculation and Debugging Script.

This script processes and analyzes hockey game data to calculate Corsi statistics for players
and teams across multiple seasons. It includes logic for tracking player shifts, penalty
exclusions, and Corsi event tallying.

Key Features:
- Tracks player shifts and ensures correct exclusion of penalty times.
- Calculates individual player and team-level Corsi statistics.
- Processes multiple seasons and outputs results to CSV files.
- Provides detailed logging for debugging.

Author: Eric Winiecke
Date: October 30, 2024
"""

import os

import pandas as pd

from constants import SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger
from schema_utils import fq

# engine = get_db_engine()

LOG_FILE_PATH = "/Users/ericwiniecke/Documents/github/cost_cup/logs/data_processing.log"
logger = setup_logger(LOG_FILE_PATH)
logger.info("Logger configured successfully. Test message to ensure logging works.")


def get_num_players(shift_df):
    """
    Computes the number of players on ice at each recorded time instance.

    This function processes shift data to determine how many players are on the ice
    at each shift start and end time, tracking changes cumulatively.

    Args:
    ----
        shift_df (pd.DataFrame): DataFrame containing shift data with columns:
                                 ['game_id', 'player_id', 'shift_start', 'shift_end'].

    Returns:
    -------
        pd.DataFrame: DataFrame with columns:
                      ['value' (time), 'num_players' (player count at that time)].
                      Only rows where the number of players changes are included.

    """  # noqa: D401
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


def get_penalty_exclude_times(
    game_shifts: pd.DataFrame, game_skater_stats: pd.DataFrame | None = None
):
    """
    Determine time periods where events should be excluded due to penalties.

    If `team_id` is already present in `game_shifts`, it will be used directly.
    Otherwise, `game_skater_stats` is required to merge team_id onto shifts.
    """
    if game_shifts.empty:
        logger.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
        return pd.DataFrame()

    # Ensure team_id exists on shifts (prefer shifts' team_id if already present)
    if "team_id" not in game_shifts.columns or game_shifts["team_id"].isna().any():
        if game_skater_stats is None or game_skater_stats.empty:
            raise ValueError("team_id missing in game_shifts and no game_skater_stats provided.")

        game_shifts = pd.merge(
            game_shifts,
            game_skater_stats[["game_id", "player_id", "team_id"]],
            on=["game_id", "player_id"],
            how="left",
        )

        # Handle merge suffixes safely
        if "team_id_x" in game_shifts.columns or "team_id_y" in game_shifts.columns:
            game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(
                columns={"team_id_x": "team_id"}
            )

    # Divide shifts by team
    team_1 = game_shifts.iloc[0]["team_id"]
    mask = game_shifts["team_id"] == team_1
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    df_num_players_1 = get_num_players(shifts_1).rename(
        columns={"value": "time", "num_players": "team_1"}
    )
    df_num_players_2 = get_num_players(shifts_2).rename(
        columns={"value": "time", "num_players": "team_2"}
    )

    df_exclude = (
        pd.concat([df_num_players_1, df_num_players_2])
        .sort_values("time", ignore_index=True)
        .ffill()
    )

    # Keep only rows where next time changes
    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask].reset_index(drop=True)

    # Determine exclusions based on player counts
    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = diff & missing

    return df_exclude


def calculate_and_save_corsi_stats(season_game_ids, season):
    """
    Calculates Corsi statistics for all games in a given season and saves results to a CSV file.

    This function iterates over all games, extracts relevant data, computes Corsi statistics,
    and writes the final dataset to a CSV file.

    Args:
    ----
        season_game_ids (list): List of game IDs for the given season.
        season (int): Season year (e.g., 20152016).

    Returns:
    -------
        None

    """  # noqa: D401
    df_master = {}

    engine = get_db_engine()

    try:
        plays_view = fq("derived", f"game_plays_{season}_from_raw_pbp")
        df_master["game_plays"] = pd.read_sql(f"SELECT * FROM {plays_view}", engine)

        # ✅ Load shifts WITH team_id so we don't need game_skater_stats for 20202021+
        raw_shifts_resolved = fq("raw", "raw_shifts_resolved")
        dim_team_code = fq("dim", "dim_team_code")

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

    # ✅ Only require plays + shifts
    required = {"game_plays", "game_shifts"}
    missing = [k for k in required if k not in df_master]
    if missing:
        logger.error(f"Missing required dataframes: {missing}")
        return

    # ✅ Use game ids from the plays view (regular season only)
    season_game_ids = sorted(df_master["game_plays"]["game_id"].dropna().astype(int).unique())
    logger.info(f"{season}: processing {len(season_game_ids)} games (from PBP view)")

    season_corsi_stats = []

    plays_by_game = dict(tuple(df_master["game_plays"].groupby("game_id", sort=False)))
    shifts_by_game = dict(tuple(df_master["game_shifts"].groupby("game_id", sort=False)))

    for game_id in season_game_ids:
        gp = plays_by_game.get(game_id)
        gs = shifts_by_game.get(game_id)

        if gp is None or gs is None or gp.empty or gs.empty:
            logger.warning(f"Skipping game {game_id}: missing plays/shifts.")
            continue

        df_game = {"game_plays": gp, "game_shifts": gs}

        # ✅ Seed Corsi rows from shifts (has team_id)
        df_corsi = gs[["game_id", "player_id", "team_id"]].drop_duplicates().copy()

        corsi_stats = create_corsi_stats(df_corsi, df_game)
        if corsi_stats is not None and not corsi_stats.empty:
            season_corsi_stats.append(corsi_stats)

    if season_corsi_stats:
        final_season_df = pd.concat(season_corsi_stats, ignore_index=True)
        output_dir = os.path.join(os.getcwd(), "corsi_stats")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"corsi_stats_{season}.csv")
        final_season_df.to_csv(output_file, index=False)
        logger.info(f"Saved Corsi data for {season} to {output_file}.")
    else:
        logger.warning(f"No valid Corsi data was generated for the {season} season.")


def organize_by_season(df):
    """
    Filter and process hockey data by season.

    Extract and organize data for specific seasons, merge necessary statistics,
    and perform Corsi calculations.

    Args:
    ----
        SEASONS_MODERN global constant(list): List of season years (e.g., [20182019, 20192020,...]).
        df (dict): Dictionary of DataFrames containing game data.

    Returns:
    -------
        list: A list containing processed DataFrames for each season.

    """
    df_orig, nhl_dfs = df, []
    game_id = 2015020002

    for season in SEASONS_MODERN:
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season} and game_id == {game_id}")
        if df["game"].empty:
            logger.warning(f"Game ID {game_id} not found in season {season}.")
            continue

        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            df[name] = pd.merge(
                df[name][df[name]["game_id"] == game_id],
                df["game"][["game_id"]],
                on="game_id",
            ).drop_duplicates()

        df_corsi = df["game_skater_stats"].sort_values(["game_id", "player_id"], ignore_index=True)[
            ["game_id", "player_id", "team_id"]
        ]
        nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])

    return nhl_dfs


def prepare_game_plays(df, relevant_events):
    """
    Prepare the game_plays DataFrame for Corsi event processing.

    Create a 'time' column based on period and periodTime, filter for relevant
    event types, and drop rows with missing team information.

    Args:
    ----
        df (dict): Dictionary of DataFrames, including 'game_plays'.
        relevant_events (list): List of event types to retain (e.g., ["Shot", "Goal"]).

    Returns:
    -------
        pd.DataFrame or None: Cleaned and filtered game_plays DataFrame, or None if
        required data is missing.

    """
    if "game_plays" not in df:
        logger.error("'game_plays' DataFrame missing in df.")
        return None

    gp = df["game_plays"]

    if "periodTime" not in gp.columns or "period" not in gp.columns:
        logger.error("'periodTime' or 'period' column missing in game_plays.")
        return None

    gp["time"] = gp["periodTime"] + (gp["period"] - 1) * 1200
    logger.info("Calculated 'time' column in game_plays.")

    gp = gp[gp["event"].isin(relevant_events)].dropna(subset=["team_id_for", "team_id_against"])

    if "time" not in gp.columns:
        logger.error("The 'time' column is missing after filtering.")
        return None

    return gp


def calculate_corsi_for_game(df_corsi, game_id, df, game_plays):
    """Calculate Corsi statistics for a single game."""  # noqa: E501
    game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
    plays_game = game_plays.query(f"game_id == {game_id}")

    if plays_game.empty or game_shifts.empty:
        return df_corsi

    if "time" not in plays_game.columns:
        logger.error("'time' column missing in plays_game after filtering by game_id.")
        return df_corsi

    # Only merge team_id if shifts don't already have it (and gss exists)
    if "team_id" not in game_shifts.columns:
        gss_df = df.get("game_skater_stats")
        if gss_df is not None and not gss_df.empty:
            game_shifts = pd.merge(
                game_shifts,
                gss_df[["game_id", "player_id", "team_id"]],
                on=["game_id", "player_id"],
                how="left",
            )

    game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
    game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

    gss = df.get("game_skater_stats")  # can be None
    df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(drop=True)

    idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
    idx[idx < 0] = 0
    idx = idx.clip(0, len(df_num_players) - 1)

    mask = df_num_players["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
    plays_game = plays_game.loc[~mask]

    for _, event in plays_game.iterrows():
        df_corsi = update_corsi(df_corsi, event, game_shifts)

    return df_corsi


def update_corsi(df_corsi, event, game_shifts):
    """
    Update player-level Corsi statistics based on a single game event.

    Identify players on the ice at the time of the event and apply the appropriate
    Corsi For and Corsi Against adjustments depending on the event type.

    Args:
    ----
        df_corsi (pd.DataFrame): DataFrame tracking player-level Corsi stats.
        event (pd.Series): A row from the game_plays DataFrame representing a single event.
        game_shifts (pd.DataFrame): DataFrame of player shifts for the current game.

    Returns:
    -------
        pd.DataFrame: Updated Corsi DataFrame with stats modified based on the event.

    """
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
        df_corsi.loc[df_corsi["player_id"].isin(players_for["player_id"]), "corsi_against"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against["player_id"]), "corsi_for"] += 1

    return df_corsi


def create_corsi_stats(df_corsi, df):
    """
    Generate Corsi statistics for all players across multiple games.

    Prepare event data, filter for relevant Corsi events, and update player-level
    Corsi For and Against counts based on play-by-play and shift data. Computes
    overall Corsi and Corsi For Percentage (CF%) for each player.

    Args:
    ----
        df_corsi (pd.DataFrame): DataFrame with player and game IDs to be updated with
            Corsi statistics.
        df (dict): Dictionary of DataFrames containing game data, including 'game_plays',
            'game_shifts', and 'game_skater_stats'.

    Returns:
    -------
        pd.DataFrame: Updated Corsi DataFrame with 'corsi_for', 'corsi_against', 'corsi',
            and 'CF_Percent' columns.

    """
    logger.info("Entered create_corsi_stats")

    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
    game_plays = prepare_game_plays(df, relevant_events)

    if game_plays is None:
        return df_corsi

    game_id_prev = None
    for row in df_corsi.itertuples(index=False):
        game_id = row.game_id
        if game_id != game_id_prev:
            game_id_prev = game_id
            df_corsi = calculate_corsi_for_game(df_corsi, game_id, df, game_plays)

    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
    df_corsi["CF_Percent"] = (
        ((df_corsi["corsi_for"] / (df_corsi["corsi_for"] + df_corsi["corsi_against"])) * 100)
        .fillna(0)
        .round(4)
    )

    logger.info("Corsi calculations complete. Summary:")
    logger.info(df_corsi.head(5))
    return df_corsi


if __name__ == "__main__":
    for season in SEASONS_MODERN:
        logger.info(f"Running season {season}")
        calculate_and_save_corsi_stats([], season)
