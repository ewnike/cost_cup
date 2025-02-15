"""
January 5, 2025
Code to calculate shot statistics
per team per season.
Eric Winiecke
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd

from load_data import get_env_vars, load_data

# Set up logging with explicit confirmation of path
# Define the log file path
log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing_II.log"
log_directory = os.path.dirname(log_file_path)

# Ensure the log directory exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    print(f"Created log directory: {log_directory}")
else:
    print(f"Log directory exists: {log_directory}")

# Set up RotatingFileHandler (Max size 5 MB, keep up to 3 backup files)
rotating_handler = RotatingFileHandler(
    log_file_path, maxBytes=5 * 1024 * 1024, backupCount=3
)
rotating_handler.setLevel(logging.INFO)

# Define log format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
rotating_handler.setFormatter(formatter)

# Set up root logger with the rotating handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(rotating_handler)
logger.addHandler(logging.StreamHandler())  # Also print logs to console

# Test logging configuration
logger.info("Logger configured successfully with RotatingFileHandler.")
print(f"Logging to file: {log_file_path}")


def get_season_game_ids(df_game, season):
    """
    Retrieve all game IDs for a given season.
    """
    return df_game[df_game["season"] == season]["game_id"].tolist()


def organize_data_by_season(df_master, season_game_ids):
    """
    Organize dataframes for all games in a season.
    """
    organized_data = {
        "game_shifts": df_master["game_shifts"][
            df_master["game_shifts"]["game_id"].isin(season_game_ids)
        ],
        "game_plays": df_master["game_plays"][
            df_master["game_plays"]["game_id"].isin(season_game_ids)
        ],
        "game_skater_stats": df_master["game_skater_stats"][
            df_master["game_skater_stats"]["game_id"].isin(season_game_ids)
        ],
    }

    # Ensure cumulative time is calculated for game_plays
    if "time" not in organized_data["game_plays"].columns:
        if "periodTime" in organized_data["game_plays"].columns:
            organized_data["game_plays"]["time"] = (
                organized_data["game_plays"]["periodTime"]
                + (organized_data["game_plays"]["period"] - 1) * 1200
            )
        else:
            logging.warning(
                "Column 'periodTime' is missing from game_plays. Skipping time calculation."
            )

    # Ensure cumulative time is calculated for game_shifts (if applicable)
    if "time" not in organized_data["game_shifts"].columns:
        if "periodTime" in organized_data["game_shifts"].columns:
            organized_data["game_shifts"]["time"] = (
                organized_data["game_shifts"]["periodTime"]  # If periodTime exists
                + (organized_data["game_shifts"]["period"] - 1) * 1200
            )
        else:
            logging.info(
                "'periodTime' not present in game_shifts. Skipping time calculation."
            )

    return organized_data


def preprocess_exclude_times(organized_data, season_game_ids):
    """
    Compute penalty exclude times for all games in a season.

    Args:
        organized_data (dict): Organized data for the season.
        season_game_ids (list): List of game IDs for the season.

    Returns:
        pd.DataFrame: Combined penalty exclude times for all games.
    """
    try:
        all_exclude_times = []
        game_shifts = organized_data["game_shifts"]
        game_skater_stats = organized_data["game_skater_stats"]
        game_plays = organized_data["game_plays"]

        # Process each game
        for game_id in season_game_ids:
            shifts = game_shifts[game_shifts["game_id"] == game_id]
            skater_stats = game_skater_stats[game_skater_stats["game_id"] == game_id]
            plays = game_plays[game_plays["game_id"] == game_id]

            # Skip games with missing data
            if shifts.empty or skater_stats.empty or plays.empty:
                logging.warning(f"Skipping game_id {game_id} due to missing data.")
                continue

            # Compute penalty exclude times for the game
            df_exclude = get_penalty_exclude_times(shifts, skater_stats, plays)
            all_exclude_times.append(df_exclude)

        # Combine all games into one DataFrame
        if all_exclude_times:
            exclude_times = pd.concat(all_exclude_times, ignore_index=True)
        else:
            exclude_times = pd.DataFrame()  # Return an empty DataFrame if no data

        return exclude_times

    except Exception as e:
        logging.error(f"Error in preprocessing exclude times: {e}")
        raise


# Helper Functions
def verify_penalty(game_id, time, game_plays, game_shifts):
    """
    Verify if a penalty exists at the given time, checking for offsetting minors.

    Args:
        game_id (int): The ID of the game to verify.
        time (float): The time in seconds to check.
        game_plays (pd.DataFrame): The DataFrame containing game play events.
        game_shifts (pd.DataFrame): The DataFrame containing game shifts.

    Returns:
        str: 'Penalty' for regular penalties, 'Offsetting' for offsetting minors, 'None' otherwise.
    """
    # Ensure required columns exist
    required_columns = [
        "team_id_for",
        "team_id_against",
        "period",
        "periodTime",
        "event",
    ]
    for col in required_columns:
        if col not in game_plays.columns:
            logging.error(f"Missing column '{col}' in game_plays.")
            return "None"

    # Calculate event_time if it doesn't already exist
    if "event_time" not in game_plays.columns:
        game_plays["event_time"] = (game_plays["period"] - 1) * 1200 + game_plays[
            "periodTime"
        ]
        logging.info("Calculated 'event_time' column in game_plays.")

    # Filter game plays by game_id
    plays = game_plays[game_plays["game_id"] == game_id]

    # Check for penalties at the given time
    penalties = plays[(plays["event"] == "Penalty") & (plays["event_time"] == time)]

    if penalties.empty:
        return "None"

    # Check if penalties are offsetting
    unique_teams = penalties["team_id_for"].nunique()
    if unique_teams > 1:
        return "Offsetting"

    # If a single penalty exists
    return "Penalty"


def get_num_players(shift_df):
    shifts_melted = pd.melt(
        shift_df,
        id_vars=["game_id", "player_id"],
        value_vars=["shift_start", "shift_end"],
    ).sort_values("value", ignore_index=True)
    shifts_melted["change"] = (
        2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
    )
    shifts_melted["num_players"] = shifts_melted["change"].cumsum()
    df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()
    return df_num_players[
        df_num_players["num_players"].shift() != df_num_players["num_players"]
    ].reset_index(drop=True)


def get_penalty_exclude_times(game_shifts, game_skater_stats, game_plays):
    if game_shifts.empty:
        logging.warning("game_shifts is empty.")
        return pd.DataFrame()

    game_shifts = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left",
    )
    game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(
        columns={"team_id_x": "team_id"}
    )

    # Divide shifts by team
    team_1 = game_shifts.iloc[0]["team_id"]
    shifts_1 = game_shifts[game_shifts["team_id"] == team_1]
    shifts_2 = game_shifts[game_shifts["team_id"] != team_1]

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

    # Ensure `game_id` is preserved
    df_exclude["game_id"] = game_shifts["game_id"].iloc[0]

    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask]

    exclude_list = []
    for _, row in df_exclude.iterrows():
        # Safeguard against missing `game_id`
        if "game_id" not in row or pd.isna(row["game_id"]):
            logging.error("Missing game_id in df_exclude row.")
            exclude_list.append(False)
            continue

        penalty_type = verify_penalty(
            row["game_id"], row["time"], game_plays, game_shifts
        )
        if penalty_type == "Penalty":
            exclude_list.append(True)
        elif penalty_type == "Offsetting":
            exclude_list.append(False)
        else:
            exclude = (
                (row["team_1"] != row["team_2"])
                & (row["team_1"] <= 6)
                & (row["team_2"] <= 6)
            )
            exclude_list.append(exclude)

    df_exclude["exclude"] = exclude_list
    print(df_exclude.head())
    return df_exclude.reset_index(drop=True)


def assemble_arrays_for_processing(organized_data, exclude_times):
    """
    Assemble arrays of processed data for all game IDs in a season.

    Args:
        organized_data (dict): Dictionary containing game_shifts, game_plays, and game_skater_stats as DataFrames.
        exclude_times (pd.DataFrame): DataFrame containing penalty exclude times for all game IDs.

    Returns:
        list: A list of tuples, where each tuple contains a game_id and the corresponding filtered plays DataFrame.
    """
    all_data = []

    # Verify the structure of organized_data
    if not isinstance(organized_data, dict):
        raise ValueError("organized_data should be a dictionary.")
    if (
        "game_plays" not in organized_data
        or "game_shifts" not in organized_data
        or "game_skater_stats" not in organized_data
    ):
        raise KeyError(
            "Missing required keys in organized_data (game_plays, game_shifts, game_skater_stats)."
        )
    if exclude_times.empty:
        raise ValueError("Exclude times data is empty.")

    # Get unique game IDs from game_plays
    unique_game_ids = organized_data["game_plays"]["game_id"].unique()
    print(f"Unique game IDs in game_plays: {unique_game_ids}")

    # Loop through each game_id
    for game_id in unique_game_ids:
        print(f"Processing game_id: {game_id}")

        # Filter data for the current game_id
        exclude_time = exclude_times[exclude_times["game_id"] == game_id]
        plays = organized_data["game_plays"][
            organized_data["game_plays"]["game_id"] == game_id
        ]

        # Check if either DataFrame is empty
        if exclude_time.empty or plays.empty:
            logging.warning(
                f"Skipping game_id {game_id} due to empty exclude_time or plays DataFrame."
            )
            continue

        # Debugging checks
        print("exclude_time columns:", exclude_time.columns)
        print("plays columns:", plays.columns)

        # Check for 'time' column
        if "time" not in exclude_time.columns or "time" not in plays.columns:
            raise KeyError(
                f"'time' column is missing in exclude_time or plays for game_id: {game_id}"
            )

        # Perform exclusion based on exclude_times
        idx = exclude_time["time"].searchsorted(plays["time"]) - 1
        idx[idx < 0] = 0  # Ensure no negative indices
        # Ensure indices are within a valid range
        idx = idx.clip(0, len(exclude_time) - 1)

        # Debugging check for indices
        print(f"Clipped idx values: {idx}")

        # Apply mask to filter plays
        mask = exclude_time["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
        plays = plays.loc[~mask]

        # Append filtered data for this game_id
        all_data.append((game_id, plays))

    return all_data


def process_season_team_event_totals(assembled_data):
    """
    Process team event totals for the entire season and calculate season-level statistics.

    Args:
        assembled_data (list): A list of tuples (game_id, game_data) where game_data contains play events.

    Returns:
        tuple: A tuple containing:
            - DataFrame with game-level results for each team.
            - DataFrame with season-level totals for each team.
    """
    all_results = []

    # Process each game's data
    for game_id, game_data in assembled_data:
        even_strength_plays = (
            game_data  # Assuming this is already filtered for even strength
        )
        game_totals_for = (
            even_strength_plays.groupby("team_id_for")
            .agg(
                total_goals=("event", lambda x: (x == "Goal").sum()),
                total_shots=("event", lambda x: (x == "Shot").sum()),
                total_missed_shots=("event", lambda x: (x == "Missed Shot").sum()),
                total_blocked_shots_for=(
                    "event",
                    lambda x: (x == "Blocked Shot").sum(),
                ),
            )
            .reset_index()
            .rename(columns={"team_id_for": "team_id"})
        )

        game_totals_against = (
            even_strength_plays.groupby("team_id_against")
            .agg(
                total_goals_against=("event", lambda x: (x == "Goal").sum()),
                total_shots_against=("event", lambda x: (x == "Shot").sum()),
                total_missed_shots_against=(
                    "event",
                    lambda x: (x == "Missed Shot").sum(),
                ),
                total_blocked_shots_against=(
                    "event",
                    lambda x: (x == "Blocked Shot").sum(),
                ),
            )
            .reset_index()
            .rename(columns={"team_id_against": "team_id"})
        )

        # Merge results for this game
        game_results = pd.merge(
            game_totals_for, game_totals_against, on="team_id", how="outer"
        ).fillna(0)
        game_results["game_id"] = game_id
        all_results.append(game_results)

    # Combine all game-level results
    season_results = pd.concat(all_results, ignore_index=True)

    # Aggregate season totals by team
    season_totals = season_results.groupby("team_id").sum().reset_index()

    # Calculate CF, CA, and CF%
    season_totals["CF"] = (
        season_totals["total_goals"]
        + season_totals["total_shots"]
        + season_totals["total_missed_shots"]
        + season_totals["total_blocked_shots_for"]
    )
    season_totals["CA"] = (
        season_totals["total_goals_against"]
        + season_totals["total_shots_against"]
        + season_totals["total_missed_shots_against"]
        + season_totals["total_blocked_shots_against"]
    )
    season_totals["CF%"] = season_totals["CF"] / (
        season_totals["CF"] + season_totals["CA"]
    )

    return season_results, season_totals


if __name__ == "__main__":
    # Step 1: Load environment variables and data
    env_vars = get_env_vars()
    df_master = load_data(env_vars)
    df_game = df_master["game"]

    # Step 2: Define the seasons to process (as integers)
    seasons = [20152016, 20162017, 20172018]  # Ensure these are integers

    # Directory to save results
    output_dir = "team_event_totals"
    os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist

    try:
        for season in seasons:
            print(f"Processing season {season}")

            # Step 3: Get game IDs for the season
            season_game_ids = df_game[df_game["season"] == season]["game_id"].unique()
            if not season_game_ids.size:
                print(f"No games found for season {season}. Skipping...")
                continue
            print(f"Found {len(season_game_ids)} games for season {season}")

            # Step 4: Organize data by season
            organized_data = organize_data_by_season(df_master, season_game_ids)
            if organized_data["game_plays"].empty:
                print(f"No game_plays data found for season {season}. Skipping...")
                continue

            # Step 5: Compute penalty exclude times
            exclude_times = preprocess_exclude_times(organized_data, season_game_ids)
            if exclude_times.empty:
                print(
                    f"No penalty exclude times computed for season {season}. Skipping..."
                )
                continue
            exclude_file = os.path.join(
                output_dir, f"penalty_exclude_times_{season}.csv"
            )
            exclude_times.to_csv(exclude_file, index=False)
            print(f"Penalty exclude times saved to {exclude_file}")

            # Step 6: Assemble arrays for processing
            assembled_data = assemble_arrays_for_processing(
                organized_data, exclude_times
            )
            if not assembled_data:
                print(f"No assembled data for season {season}. Skipping...")
                continue

            # Step 7: Process season totals (game-level and season-level)
            season_results, season_totals = process_season_team_event_totals(
                assembled_data
            )

            # Save game-level results to a CSV file
            game_results_file = os.path.join(
                output_dir, f"team_event_totals_games_{season}.csv"
            )
            season_results.to_csv(game_results_file, index=False)
            print(f"Game-level results saved to {game_results_file}")

            # Save season-level totals to another CSV file
            season_totals_file = os.path.join(
                output_dir, f"team_event_totals_season_{season}.csv"
            )
            season_totals.to_csv(season_totals_file, index=False)
            print(f"Season-level totals saved to {season_totals_file}")

        print("Processing for all seasons complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
