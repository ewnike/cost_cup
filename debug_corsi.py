"""
October 30, 2024.
Code to test and debug Corsi Calculations.
Tested tracking a single player and all players
Logic for tallying Blocked Shots assigns correctly.
Blocked For is -1 for the blocking team and a +1 for
the against team. (Corsi is an offensive statistic that
has a main purpose of calculating all shot attempts
taken when a player is on the ice. Not a defensive stat)
"""



import logging
from logging.handlers import RotatingFileHandler
import os
import time
from time import perf_counter

import numpy as np
import pandas as pd

from load_data import get_env_vars, load_data, get_db_engine, fetch_game_ids_20152016

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


# Function Definitions
# i like this version the best so far....
# def get_penalty_exclude_times(game_shifts, game_skater_stats):
#     if game_shifts.empty:
#         logging.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
#         return pd.DataFrame()  # Return an empty DataFrame if no shifts are available

#     # Merge the `team_id` column from `game_skater_stats` into `game_shifts`
#     game_shifts = pd.merge(
#         game_shifts,
#         game_skater_stats[["game_id", "player_id", "team_id"]],
#         on=["game_id", "player_id"],
#         how="left",
#     )
#     game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(
#         columns={"team_id_x": "team_id"}
#     )

#     # Divide shifts by team
#     team_1 = game_shifts.iloc[0]["team_id"]
#     mask = game_shifts["team_id"] == team_1
#     shifts_1 = game_shifts[mask]
#     shifts_2 = game_shifts[~mask]

#     # Calculate the number of players on each team
#     df_num_players_1 = get_num_players(shifts_1)
#     df_num_players_2 = get_num_players(shifts_2)

#     # Rename and merge the player counts for each team
#     df_num_players_1 = df_num_players_1.rename(
#         columns={"value": "time", "num_players": "team_1"}
#     )
#     df_num_players_2 = df_num_players_2.rename(
#         columns={"value": "time", "num_players": "team_2"}
#     )

#     df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values(
#         "time", ignore_index=True
#     )
#     df_exclude = df_exclude.ffill()

#     mask = df_exclude["time"].shift(-1) != df_exclude["time"]
#     df_exclude = df_exclude[mask]

#     # Identify overlaps (more than 6 players on a team)
#     overlaps = (df_exclude["team_1"] > 6) | (df_exclude["team_2"] > 6)
#     if overlaps.any():
#         logging.info("Detected overlaps during line changes:")
#         for _, row in df_exclude[overlaps].iterrows():
#             logging.info(
#                 f"Time: {row['time']}, Team 1 Players: {row['team_1']}, Team 2 Players: {row['team_2']}"
#             )

#     # Determine exclusions based on player counts
#     unequal_players = df_exclude["team_1"] != df_exclude["team_2"]
#     # below_minimum = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
#     reasonable_max_players = (df_exclude["team_1"] <= 6) & (df_exclude["team_2"] <= 6)

#     # Final exclusion condition
#     # df_exclude["exclude"] = unequal_players & below_minimum & reasonable_max_players
#     # Updated exclusion condition
#     df_exclude["exclude"] = unequal_players & reasonable_max_players

#     # Reset index for clean output
#     df_exclude = df_exclude.reset_index(drop=True)

#     # Log the penalty exclude times for verification
#     logging.info("Penalty Exclude Times:")
#     for _, row in df_exclude.iterrows():
#         logging.info(
#             f"Time: {row['time']}, Team 1 Players: {row['team_1']}, Team 2 Players: {row['team_2']}, Exclude: {row['exclude']}"
#         )

#     return df_exclude
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
    required_columns = ["team_id_for", "team_id_against", "period", "periodTime", "event"]
    for col in required_columns:
        if col not in game_plays.columns:
            logging.error(f"Missing column '{col}' in game_plays.")
            return "None"

    # Filter game plays by game_id
    plays = game_plays[game_plays["game_id"] == game_id]

    # Calculate total game time in seconds
    plays["event_time"] = (plays["period"] - 1) * 1200 + plays["periodTime"]

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



def get_penalty_exclude_times(game_shifts, game_skater_stats, game_plays):
    if game_shifts.empty:
        logging.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
        return pd.DataFrame()  # Return an empty DataFrame if no shifts are available

    # Merge the `team_id` column from `game_skater_stats` into `game_shifts`
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
    mask = game_shifts["team_id"] == team_1
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    # Calculate the number of players on each team
    df_num_players_1 = get_num_players(shifts_1)
    df_num_players_2 = get_num_players(shifts_2)

    # Rename and merge the player counts for each team
    df_num_players_1 = df_num_players_1.rename(
        columns={"value": "time", "num_players": "team_1"}
    )
    df_num_players_2 = df_num_players_2.rename(
        columns={"value": "time", "num_players": "team_2"}
    )

    df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values(
        "time", ignore_index=True
    )
    df_exclude["game_id"] = game_shifts["game_id"].iloc[0]

    df_exclude = df_exclude.ffill()

    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask]

    # Identify overlaps (more than 6 players on a team)
    overlaps = (df_exclude["team_1"] > 6) | (df_exclude["team_2"] > 6)
    if overlaps.any():
        logging.info("Detected overlaps during line changes:")
        for _, row in df_exclude[overlaps].iterrows():
            logging.info(
                f"Time: {row['time']}, Team 1 Players: {row['team_1']}, Team 2 Players: {row['team_2']}"
            )

    # Calculate exclusions
    exclude_list = []
    for _, row in df_exclude.iterrows():
        game_id = row["game_id"]
        time = row["time"]
        team_1 = row["team_1"]
        team_2 = row["team_2"]

        # Check penalty type
        penalty_type = verify_penalty(game_id, time, game_plays, game_shifts)

        if penalty_type == "Penalty":
            exclude_list.append(True)  # Exclude regular penalties
            continue
        elif penalty_type == "Offsetting":
            exclude_list.append(False)  # Do not exclude offsetting minors
            continue

        # Exclude based on imbalance and reasonable player counts
        unequal_players = team_1 != team_2
        reasonable_max_players = (team_1 <= 6) & (team_2 <= 6)
        exclude = unequal_players & reasonable_max_players

        exclude_list.append(exclude)

    # Assign exclude_list to df_exclude["exclude"]
    df_exclude["exclude"] = exclude_list

    # Reset index for clean output
    df_exclude = df_exclude.reset_index(drop=True)

    # Log the penalty exclude times for verification
    logging.info("Penalty Exclude Times:")
    for _, row in df_exclude.iterrows():
        logging.info(
            f"Time: {row['time']}, Team 1 Players: {row['team_1']}, Team 2 Players: {row['team_2']}, Exclude: {row['exclude']}"
        )

    return df_exclude





#previous code that worked but may not be totally correct
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
    print(df_num_players)


# def get_penalty_exclude_times(game_shifts, game_skater_stats):
#     if game_shifts.empty:
#         logging.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
#         return pd.DataFrame()  # Return an empty DataFrame if no shifts are available

#     # Merge the `team_id` column from `game_skater_stats` into `game_shifts`
#     game_shifts = pd.merge(
#         game_shifts,
#         game_skater_stats[["game_id", "player_id", "team_id"]],
#         on=["game_id", "player_id"],
#         how="left",
#     )
#     game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(
#         columns={"team_id_x": "team_id"}
#     )

#     # Divide shifts by team
#     team_1 = game_shifts.iloc[0]["team_id"]
#     mask = game_shifts["team_id"] == team_1
#     shifts_1 = game_shifts[mask]
#     shifts_2 = game_shifts[~mask]

#     # Calculate the number of players on each team and proceed as before
#     df_num_players_1 = get_num_players(shifts_1)
#     df_num_players_2 = get_num_players(shifts_2)

#     # Rename and merge the player counts for each team
#     df_num_players_1 = df_num_players_1.rename(
#         columns={"value": "time", "num_players": "team_1"}
#     )
#     df_num_players_2 = df_num_players_2.rename(
#         columns={"value": "time", "num_players": "team_2"}
#     )

#     df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values(
#         "time", ignore_index=True
#     )
#     df_exclude = df_exclude.ffill()

#     mask = df_exclude["time"].shift(-1) != df_exclude["time"]
#     df_exclude = df_exclude[mask]

#     # Determine exclusions based on player counts
#     diff = df_exclude["team_1"] != df_exclude["team_2"]
#     missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
#     df_exclude["exclude"] = diff & missing
#     df_exclude = df_exclude.reset_index(drop=True)
#     print(df_exclude)
#     # Log the penalty exclude times for verification
#     logging.info("Penalty Exclude Times:")
#     for _, row in df_exclude.iterrows():
#         logging.info(
#             f"Time: {row['time']}, Team 1 Players: {row['team_1']}, Team 2 Players: {row['team_2']}, Exclude: {row['exclude']}"
#         )

#     return df_exclude


# Save this!!! This is the corsi code for all players.

def calculate_and_save_corsi_stats(game_id=None):
    env_vars = get_env_vars()
    df_master = load_data(env_vars)
    if "game_plays" not in df_master:
        logging.error("'game_plays' not found in loaded data.")
        return

    # Filter data by game_id if provided
    if game_id:
        df_master = {
            name: df[df["game_id"] == game_id] for name, df in df_master.items()
        }
    logging.info(f"Data loaded for game_id {game_id}.")

    # Load `game_plays` and create `time` column if it doesn't exist
    game_plays = df_master.get("game_plays")
    if "time" not in game_plays.columns:
        game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
        logging.info("Calculated 'time' column in game_plays.")

    # Log Penalty events
    penalties = game_plays[game_plays["event"] == "Penalty"]
    for _, row in penalties.iterrows():
        logging.info(
            f"Penalty Event - Game ID: {row['game_id']}, Time: {row['time']}, Team: {row['team_id_for']}"
        )

    # Load shifts and ensure team_id is added for each player in shifts
    if "game_shifts" in df_master and "game_skater_stats" in df_master:
        game_shifts = pd.merge(
            df_master["game_shifts"],
            df_master["game_skater_stats"][["game_id", "player_id", "team_id"]],
            on=["game_id", "player_id"],
            how="left"
        )

    # Track CF and CA for each player across all shifts in a game
    players = game_shifts["player_id"].unique()
    player_stats = {player_id: {"CF": 0, "CA": 0, "Blocked For": 0, "Blocked Against": 0} for player_id in players}

    # Calculate CF/CA and track Blocked Shots for each player
    for player_id in players:
        player_shifts = game_shifts[game_shifts["player_id"] == player_id]
        player_team_id = player_shifts["team_id"].iloc[0] if not player_shifts.empty else None

        if player_team_id is None:
            logging.warning(f"No team_id found for player_id {player_id}. Skipping Corsi calculation for this player.")
            continue

        # Process each shift for CF/CA tallying including Blocked Shots
        for _, shift in player_shifts.iterrows():
            shift_cf = 0
            shift_ca = 0
            shift_blocked_for = 0
            shift_blocked_against = 0

            # Define shift time boundaries
            shift_start = shift["shift_start"]
            shift_end = shift["shift_end"]

            # Find all plays within this shift's duration
            plays_during_shift = game_plays[
                (game_plays["time"] >= shift_start) & (game_plays["time"] <= shift_end)
            ]

            # Tally Corsi for each play within the shift duration
            for _, play in plays_during_shift.iterrows():
                if play["event"] in ["Shot", "Goal", "Missed Shot"]:  # Standard Corsi events
                    if play["team_id_for"] == player_team_id:  # Corsi For
                        shift_cf += 1
                    elif play["team_id_against"] == player_team_id:  # Corsi Against
                        shift_ca += 1
                elif play["event"] == "Blocked Shot":  # Special handling for Blocked Shots
                    if play["team_id_for"] == player_team_id:  # Blocked Shot Against
                        shift_blocked_against += 1
                        shift_ca += 1  # Count as Corsi Against
                    elif play["team_id_against"] == player_team_id:  # Blocked Shot For
                        shift_blocked_for += 1
                        shift_cf += 1  # Count as Corsi For

            # Update player cumulative stats
            player_stats[player_id]["CF"] += shift_cf
            player_stats[player_id]["CA"] += shift_ca
            player_stats[player_id]["Blocked For"] += shift_blocked_for
            player_stats[player_id]["Blocked Against"] += shift_blocked_against

            # Log shift details including blocked shots
            logging.info(
                f"Player ID: {player_id}, Shift - Game ID: {shift['game_id']}, Start: {shift_start}, End: {shift_end}, "
                f"Shift CF: {shift_cf}, Shift CA: {shift_ca}, "
                f"Shift Blocked Shots For: {shift_blocked_for}, Shift Blocked Shots Against: {shift_blocked_against}, "
                f"Cumulative CF: {player_stats[player_id]['CF']}, Cumulative CA: {player_stats[player_id]['CA']}, "
                f"Cumulative Blocked Shots For: {player_stats[player_id]['Blocked For']}, "
                f"Cumulative Blocked Shots Against: {player_stats[player_id]['Blocked Against']}"
            )

    # Log Blocked Shot Events for auditing
    blocked_shot_events = game_plays[game_plays["event"] == "Blocked Shot"]
    for index, event in blocked_shot_events.iterrows():
        logging.info(
            f"Blocked Shot Event - Game ID: {event['game_id']}, Time: {event['time']}, "
            f"Team For: {event['team_id_for']}, Team Against: {event['team_id_against']}"
        )



"This is the corsi code for just 1 player"


# def calculate_and_save_corsi_stats(game_id=None, track_player_id=None):
#     env_vars = get_env_vars()
#     df_master = load_data(env_vars)

#     # Ensure necessary data is available
#     if (
#         "game_shifts" not in df_master
#         or "game_plays" not in df_master
#         or "game_skater_stats" not in df_master
#     ):
#         logging.error(
#             "Required data 'game_shifts', 'game_plays', or 'game_skater_stats' not found."
#         )
#         return

#     # Filter by game_id if provided
#     if game_id:
#         df_master = {
#             name: df[df["game_id"] == game_id] for name, df in df_master.items()
#         }

#     logging.info(f"Data loaded for game_id {game_id}.")

#     # Merge `team_id` from `game_skater_stats` into `game_shifts` to ensure it's present
#     game_shifts = pd.merge(
#         df_master["game_shifts"],
#         df_master["game_skater_stats"][["game_id", "player_id", "team_id"]],
#         on=["game_id", "player_id"],
#         how="left",
#     )

#     # Log to confirm `team_id` is present in `game_shifts`
#     if "team_id" not in game_shifts.columns:
#         logging.error(
#             "`team_id` not found in `game_shifts` after merging. Check your data."
#         )
#         return
#     else:
#         logging.info("`team_id` successfully merged into `game_shifts`.")

#     # Prepare `game_plays` with calculated `time` column if it doesn’t already exist
#     game_plays = df_master.get("game_plays")
#     if "time" not in game_plays.columns:
#         game_plays["time"] = (
#             game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
#         )
#         logging.info("Calculated 'time' column in game_plays.")

#     # Track the player’s shifts and tally Corsi metrics including blocked shots
#     if track_player_id:
#         logging.info(f"Tracking player {track_player_id} throughout the game.")

#         # Filter `game_shifts` for the specific player’s shifts
#         player_shifts = game_shifts[game_shifts["player_id"] == track_player_id]

#         # Initialize cumulative totals for CF and CA
#         cumulative_cf = 0
#         cumulative_ca = 0
#         cumulative_blocked_shots_for = 0
#         cumulative_blocked_shots_against = 0

#         # Process each shift and calculate CF/CA tallies including blocked shots
#         for _, shift in player_shifts.iterrows():
#             shift_cf = 0
#             shift_ca = 0
#             shift_blocked_for = 0
#             shift_blocked_against = 0

#             # Define shift time boundaries
#             shift_start = shift["shift_start"]
#             shift_end = shift["shift_end"]
#             player_team_id = shift["team_id"]

#             # Find plays during this shift involving the player's team
#             plays_during_shift = game_plays[
#                 (game_plays["time"] >= shift_start) & (game_plays["time"] <= shift_end)
#             ]

#             # Tally CF and CA specifically for the tracked player based on team association and event type
#             for _, play in plays_during_shift.iterrows():
#                 if play["event"] in [
#                     "Shot",
#                     "Goal",
#                     "Missed Shot",
#                 ]:  # Standard Corsi events
#                     if (
#                         play["team_id_for"] == player_team_id
#                     ):  # Corsi For for the player's team
#                         shift_cf += 1
#                     elif (
#                         play["team_id_against"] == player_team_id
#                     ):  # Corsi Against for the player's team
#                         shift_ca += 1
#                 elif (
#                     play["event"] == "Blocked Shot"
#                 ):  # Special handling for Blocked Shots
#                     if (
#                         play["team_id_for"] == player_team_id
#                     ):  # Blocked Shot Against for the player's team
#                         shift_blocked_against += 1
#                         shift_ca += 1  # Count as Corsi Against
#                     elif (
#                         play["team_id_against"] == player_team_id
#                     ):  # Blocked Shot For the player's team
#                         shift_blocked_for += 1
#                         shift_cf += 1  # Count as Corsi For

#                 # Log each relevant play during the player's shift
#                 logging.info(
#                     f"Play during Shift - Game ID: {play['game_id']}, Time: {play['time']}, "
#                     f"Event: {play['event']}, Team For: {play['team_id_for']}, Team Against: {play['team_id_against']}"
#                 )

#             # Update cumulative CF, CA, Blocked Shots For and Against for this player
#             cumulative_cf += shift_cf
#             cumulative_ca += shift_ca
#             cumulative_blocked_shots_for += shift_blocked_for
#             cumulative_blocked_shots_against += shift_blocked_against

#             # Log shift details with CF and CA tallies for the tracked player
#             logging.info(
#                 f"Shift - Game ID: {shift['game_id']}, Start: {shift_start}, End: {shift_end}, "
#                 f"Shift CF: {shift_cf}, Shift CA: {shift_ca}, "
#                 f"Shift Blocked Shots For: {shift_blocked_for}, Shift Blocked Shots Against: {shift_blocked_against}, "
#                 f"Cumulative CF: {cumulative_cf}, Cumulative CA: {cumulative_ca}, "
#                 f"Cumulative Blocked Shots For: {cumulative_blocked_shots_for}, Cumulative Blocked Shots Against: {cumulative_blocked_shots_against}"
#             )

#     # Log Blocked Shot Events for auditing
#     blocked_shot_events = game_plays[game_plays["event"] == "Blocked Shot"]
#     for index, event in blocked_shot_events.iterrows():
#         logging.info(
#             f"Blocked Shot Event - Game ID: {event['game_id']}, Time: {event['time']}, "
#             f"Team For: {event['team_id_for']}, Team Against: {event['team_id_against']}"
#         )


def organize_by_season(seasons, df):
    df_orig, nhl_dfs = df, []
    game_id = 2015020002

    for season in seasons:
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season} and game_id == {game_id}")
        if df["game"].empty:
            logging.warning(f"Game ID {game_id} not found in season {season}.")
            continue

        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            df[name] = pd.merge(
                df[name][df[name]["game_id"] == game_id],
                df["game"][["game_id"]],
                on="game_id",
            ).drop_duplicates()

        df_corsi = df["game_skater_stats"].sort_values(
            ["game_id", "player_id"], ignore_index=True
        )[["game_id", "player_id", "team_id"]]
        nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])
        print('Howdy Doody!! + *(50)')
    return nhl_dfs


def create_corsi_stats(df_corsi, df):
    logging.info("Entered create_corsi_stats")

    # Initialize Corsi statistics columns with zeros for all players
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

    # Ensure the 'time' column is created and available in game_plays
    if "game_plays" in df:
        if (
            "periodTime" in df["game_plays"].columns
            and "period" in df["game_plays"].columns
        ):
            # Calculate 'time' based on period and periodTime
            df["game_plays"]["time"] = (
                df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
            )
            logging.info("Calculated 'time' column in game_plays.")
        else:
            logging.error("'periodTime' or 'period' column missing in game_plays.")
            return

        # Filter game_plays to include only relevant events
        game_plays = df["game_plays"][df["game_plays"]["event"].isin(relevant_events)]
        game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])

        # Verify 'time' column presence after filtering
        if "time" not in game_plays.columns:
            logging.error(
                "The 'time' column is missing from game_plays after filtering."
            )
            return
        else:
            logging.info("Verified 'time' column exists in game_plays after filtering.")
    else:
        logging.error("'game_plays' DataFrame missing in df.")
        return

    game_id_prev = None
    t1 = perf_counter()

    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row.iloc[:3]

        if game_id != game_id_prev:
            game_id_prev = game_id

            # Filter game_shifts and game_plays for this game_id
            game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = df["game_plays"].query(f"game_id == {game_id}")

            # Log the 'time' column in plays_game to verify its presence
            if "time" not in plays_game.columns:
                logging.error(
                    "'time' column missing in plays_game after filtering by game_id."
                )
                return
            else:
                logging.info(
                    f"'time' column verified in plays_game for game_id {game_id}."
                )

            # Merge team_id into game_shifts and ensure team_id is an integer after merging
            game_shifts = pd.merge(
                game_shifts,
                df["game_skater_stats"][["game_id", "player_id", "team_id"]],
                on=["game_id", "player_id"],
                how="left",
            )

            game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
            game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(
                drop=True
            )

            idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
            idx[idx < 0] = 0
            mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
            plays_game = plays_game.loc[~mask]

    for _, event in plays_game.iterrows():
        event_time = event["time"]
        team_for = event["team_id_for"]
        team_against = event["team_id_against"]

        # Find all players on ice for both teams at this event time
        players_on_ice = game_shifts[
            (game_shifts["shift_start"] <= event_time)
            & (game_shifts["shift_end"] >= event_time)
        ]

        # Separate players by team
        players_for_team = players_on_ice[players_on_ice["team_id"] == team_for]
        players_against_team = players_on_ice[players_on_ice["team_id"] == team_against]

        # Log the number of players on each team at this event
        logging.info(
            f"Event: {event['event']} at time {event_time} - "
            f"Players for team {team_for} on ice: {len(players_for_team)}, "
            f"Players against team {team_against} on ice: {len(players_against_team)}"
        )

        # Log player IDs for detailed tracking
        logging.info(
            f"Player IDs for team {team_for} on ice: {players_for_team['player_id'].tolist()}"
        )
        logging.info(
            f"Player IDs for team {team_against} on ice: {players_against_team['player_id'].tolist()}"
        )

        # Corsi calculations and log updates
        if event["event"] in ["Shot", "Goal", "Missed Shot"]:
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"
            ] += 1
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]),
                "corsi_against",
            ] += 1

            logging.info(
                f"{event['event']} - CF updated for players on team {team_for} "
                f"and CA updated for players on team {team_against}"
            )

        elif event["event"] == "Blocked Shot":
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]),
                "corsi_against",
            ] += 1
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]),
                "corsi_for",
            ] += 1

            logging.info(
                f"Blocked Shot - CA updated for players on team {team_for} "
                f"and CF updated for players on team {team_against}"
            )

    # Final log summary
    logging.info("Corsi calculations complete. Summary of first few rows:")
    logging.info(df_corsi.head(5))
    return df_corsi


# def calculate_team_event_totals(game_id=None):
#     """
#     Calculate the total number of goals, shots, missed shots, and blocked shots for and against
#     each team_id, excluding periods when teams are not at even strength.
#     """
#     env_vars = get_env_vars()
#     df_master = load_data(env_vars)

#     if not all(key in df_master for key in ["game_plays", "game_shifts", "game_skater_stats"]):
#         logging.error("Required dataframes are missing from the loaded data.")
#         return

#     # Filter data by game_id if provided
#     if game_id:
#         df_master = {name: df[df["game_id"] == game_id] for name, df in df_master.items()}
#     logging.info(f"Data loaded for game_id {game_id}.")

#     # Prepare dataframes
#     game_plays = df_master["game_plays"]
#     game_shifts = df_master["game_shifts"]
#     game_skater_stats = df_master["game_skater_stats"]

#     # Create 'time' column in game_plays if it doesn't exist
#     if "time" not in game_plays.columns:
#         game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200

#     # Get exclude times when teams are not at even strength
#     #df_exclude_times = get_penalty_exclude_times(game_shifts, game_skater_stats)
#     df_exclude_times = get_penalty_exclude_times(game_shifts, game_skater_stats, game_plays)

#     idx = df_exclude_times["time"].searchsorted(game_plays["time"]) - 1
#     idx[idx < 0] = 0
#     mask = df_exclude_times["exclude"][idx].reset_index(drop=True).to_numpy()
#     even_strength_plays = game_plays.loc[~mask]

#     # Aggregate events by team_id_for
#     event_totals_for = (
#         even_strength_plays.groupby("team_id_for")
#         .agg(
#             total_goals=("event", lambda x: (x == "Goal").sum()),
#             total_shots=("event", lambda x: (x == "Shot").sum()),
#             total_missed_shots=("event", lambda x: (x == "Missed Shot").sum()),
#             total_blocked_shots_against=("event", lambda x: (x == "Blocked Shot").sum())
#         )
#         .reset_index()
#         .rename(columns={"team_id_for": "team_id"})
#     )

#     # Aggregate events by team_id_against
#     event_totals_against = (
#         even_strength_plays.groupby("team_id_against")
#         .agg(
#             total_goals_against=("event", lambda x: (x == "Goal").sum()),
#             total_shots_against=("event", lambda x: (x == "Shot").sum()),
#             total_missed_shots_against=("event", lambda x: (x == "Missed Shot").sum()),
#             total_blocked_shots_for=("event", lambda x: (x == "Blocked Shot").sum())
#         )
#         .reset_index()
#         .rename(columns={"team_id_against": "team_id"})
#     )

#     # Merge the results using 'team_id' as the key
#     event_totals_combined = pd.merge(
#         event_totals_for, event_totals_against, on="team_id", how="outer", suffixes=('_for', '_against')
#     ).fillna(0)  # Replace NaNs with 0s to handle missing data

#     # Log the final combined results
#     logging.info("Final aggregated team event totals:")
#     #logging.info(event_totals_combined)
#     logging.info("\n" + event_totals_combined.to_string(index=False))
#     logging.info("Event totals for team_id_for and team_id_against calculated and merged successfully.")
#     return event_totals_combined

def calculate_team_event_totals(game_id=None):
    """
    Calculate the total number of goals, shots, missed shots, and blocked shots for and against
    each team_id, excluding periods when teams are not at even strength.
    """
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    if not all(key in df_master for key in ["game_plays", "game_shifts", "game_skater_stats"]):
        logging.error("Required dataframes are missing from the loaded data.")
        return

    # Filter data by game_id if provided
    if game_id:
        df_master = {name: df[df["game_id"] == game_id] for name, df in df_master.items()}
    logging.info(f"Data loaded for game_id {game_id}.")

    # Prepare dataframes
    game_plays = df_master["game_plays"]
    game_shifts = df_master["game_shifts"]
    game_skater_stats = df_master["game_skater_stats"]

    # Create 'time' column in game_plays if it doesn't exist
    if "time" not in game_plays.columns:
        game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200

    # Get exclude times when teams are not at even strength
    df_exclude_times = get_penalty_exclude_times(game_shifts, game_skater_stats, game_plays)
    # print(df_exclude_times)
    idx = df_exclude_times["time"].searchsorted(game_plays["time"]) - 1
    idx[idx < 0] = 0
    mask = df_exclude_times["exclude"][idx].reset_index(drop=True).to_numpy()
    even_strength_plays = game_plays.loc[~mask]

    # Aggregate events by team_id_for
    event_totals_for = (
        even_strength_plays.groupby("team_id_for")
        .agg(
            total_goals=("event", lambda x: (x == "Goal").sum()),
            total_shots=("event", lambda x: (x == "Shot").sum()),
            total_missed_shots=("event", lambda x: (x == "Missed Shot").sum()),
            total_blocked_shots_against=("event", lambda x: (x == "Blocked Shot").sum())
        )
        .reset_index()
        .rename(columns={"team_id_for": "team_id"})
    )

    # Aggregate events by team_id_against
    event_totals_against = (
        even_strength_plays.groupby("team_id_against")
        .agg(
            total_goals_against=("event", lambda x: (x == "Goal").sum()),
            total_shots_against=("event", lambda x: (x == "Shot").sum()),
            total_missed_shots_against=("event", lambda x: (x == "Missed Shot").sum()),
            total_blocked_shots_for=("event", lambda x: (x == "Blocked Shot").sum())
        )
        .reset_index()
        .rename(columns={"team_id_against": "team_id"})
    )

    # Merge the results using 'team_id' as the key
    event_totals_combined = pd.merge(
        event_totals_for, event_totals_against, on="team_id", how="outer", suffixes=('_for', '_against')
    ).fillna(0)  # Replace NaNs with 0s to handle missing data

    # Add the game_id column
    event_totals_combined["game_id"] = game_id

    # Log the final combined results
    logging.info("Final aggregated team event totals:")
    logging.info("\n" + event_totals_combined.to_string(index=False))
    logging.info("Event totals for team_id_for and team_id_against calculated and merged successfully.")
    return event_totals_combined

#Example of calling the function
#Works for 1 game
# if __name__ == "__main__":
#     result = calculate_team_event_totals(game_id=2015020002)
#     #import os
#     print(type(result))
#     print(result)

#     print(os.getcwd())

#     # print(result)
#     columns = ['team_id','total_goals','total_shots','total_missed_shots','total_blocked_shots_against','total_goals_against','total_shots_against','total_missed_shots_against','total_blocked_shots_for']
#     game_tot = pd.DataFrame(result, columns=columns)
#     print(game_tot)
#  # Optionally, save the combined results to a CSV file
#     game_id = 2015020002
#     game_tot.to_csv(f"team_event_totals_for_game_id: {game_id}.csv", index=False)
#     print(f"Results saved to team_event_totals_for_game_id: {game_id}.csv")








# if __name__ == "__main__":
#     calculate_and_save_corsi_stats(game_id=2015020002)
    # calculate_and_save_corsi_stats(
    #     game_id=2015020002, track_player_id=8474141
    # )  # Replace with the actual player_id

# if __name__ == "__main__":
#     # Define the game IDs you want to process
#     game_ids = [2015020002, 2015020003, 2015020004, 2015020005, 2015020006]

#     all_results = []

#     for game_id in game_ids:
#         logging.info(f"Processing game_id: {game_id}")

#         try:
#             result = calculate_team_event_totals(game_id=game_id)
#             if result is not None:
#                 all_results.append(result)
#                 logging.info(f"Results for game_id {game_id}:")
#                 logging.info("\n" + result.to_string(index=False))
#             else:
#                 logging.warning(f"No data processed for game_id {game_id}.")
#         except Exception as e:
#             logging.error(f"Error processing game_id {game_id}: {e}")

#     # Combine the results from all games into a single DataFrame
#     if all_results:
#         combined_results = pd.concat(all_results, ignore_index=True)
#         logging.info("Combined results for all processed games:")
#         logging.info("\n" + combined_results.to_string(index=False))

#         # Save the combined results to a CSV file
#         output_file = "team_event_totals_for_5_games.csv"
#         combined_results.to_csv(output_file, index=False)
#         logging.info(f"Results saved to {output_file}")
#     else:
#         logging.warning("No results to combine.")

# if __name__ == "__main__":
#     # Define the game IDs you want to process
#     game_ids = [2015020002, 2015020003, 2015020004, 2015020005, 2015020006]

#     all_results = []

#     for game_id in game_ids:
#         logging.info(f"Processing game_id: {game_id}")

#         try:
#             result = calculate_team_event_totals(game_id=game_id)
#             if result is not None:
#                 all_results.append(result)
#                 logging.info(f"Results for game_id {game_id}:")
#                 logging.info("\n" + result.to_string(index=False))
#             else:
#                 logging.warning(f"No data processed for game_id {game_id}.")
#         except Exception as e:
#             logging.error(f"Error processing game_id {game_id}: {e}")

#     # Combine the results from all games into a single DataFrame
#     if all_results:
#         combined_results = pd.concat(all_results, ignore_index=True)
#         logging.info("Combined results for all processed games:")
#         logging.info("\n" + combined_results.to_string(index=False))

#         # Save the combined results to a CSV file
#         output_file = "team_event_totals_for_5_games.csv"
#         combined_results.to_csv(output_file, index=False)
#         logging.info(f"Results saved to {output_file}")
#     else:
#         logging.warning("No results to combine.")



if __name__ == "__main__":
    # Define the game IDs you want to process
    game_ids = [2015020002, 2015020003, 2015020004, 2015020005, 2015020006]
    # game_ids = [2018020157]
    all_results = []

    # Start timing for the 5 games
    start_time = time.time()

    for game_id in game_ids:
        logging.info(f"Processing game_id: {game_id}")

        try:
            result = calculate_team_event_totals(game_id=game_id)
            if result is not None:
                all_results.append(result)
                logging.info(f"Results for game_id {game_id}:")
                logging.info("\n" + result.to_string(index=False))
            else:
                logging.warning(f"No data processed for game_id {game_id}.")
        except Exception as e:
            logging.error(f"Error processing game_id {game_id}: {e}")

    # End timing for the 5 games
    end_time = time.time()

    # Calculate time taken and estimate for 1280 games
    elapsed_time = end_time - start_time
    games_processed = len(game_ids)
    estimated_total_time = (elapsed_time / games_processed) * 1280

    # Combine the results from all games into a single DataFrame
    if all_results:
        combined_results = pd.concat(all_results, ignore_index=True)
        logging.info("Combined results for all processed games:")
        logging.info("\n" + combined_results.to_string(index=False))

        # Save the combined results to a CSV file
        output_file = "team_event_totals_for_5_games.csv"
        combined_results.to_csv(output_file, index=False)
        logging.info(f"Results saved to {output_file}")
    else:
        logging.warning("No results to combine.")

    # Print timing information
    print(f"Time taken to process {games_processed} games: {elapsed_time:.2f} seconds")
    print(f"Estimated time to process 1,280 games: {estimated_total_time / 3600:.2f} hours")












