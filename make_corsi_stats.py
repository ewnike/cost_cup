# """
# July 4, 2024
# Current edition of corsi code.
# Eric Winiecke
# """

# import os
# from time import perf_counter

# import numpy as np
# import pandas as pd

# from load_data import get_env_vars, load_data


# def get_num_players(shift_df):
#     """keep track of the number of players on the ice
#     at specific times. Important for being able to assign the
#     correct shift stats per player."""
#     shifts_melted = pd.melt(
#         shift_df,
#         id_vars=["game_id", "player_id"],
#         value_vars=["shift_start", "shift_end"],
#     )
#     shifts_melted = shifts_melted.sort_values("value", ignore_index=True)
#     shifts_melted["change"] = (
#         2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
#     )
#     shifts_melted["num_players"] = shifts_melted["change"].cumsum()
#     df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()
#     mask = df_num_players["num_players"].shift() != df_num_players["num_players"]
#     df_num_players = df_num_players.loc[mask].reset_index(drop=True)

#     return df_num_players


# def get_penalty_exclude_times(game_shifts, game_skater_stats):
#     """
#     The get_penalty_exclude_times function is
#     designed to identify periods during a hockey
#     game where there is a difference in the number
#     of players on the ice between two teams,
#     particularly focusing on when a team has fewer
#     than five players (indicating a penalty situation).
#     The function processes data on player shifts and
#     game stats to determine these periods.
#     """
#     game_shifts = pd.merge(
#         game_shifts, game_skater_stats[["player_id", "team_id"]], on="player_id"
#     )
#     if len(game_shifts) == 0:
#         print("FIRE in the HOUSE")
#         print(game_shifts)

#     team_1 = game_shifts.loc[0, "team_id"]
#     mask = game_shifts["team_id"] == team_1

#     shifts_1 = game_shifts[mask]
#     shifts_2 = game_shifts[~mask]

#     df_num_players_1 = get_num_players(shifts_1)
#     df_num_players_2 = get_num_players(shifts_2)

#     df_num_players_1 = df_num_players_1.rename(
#         columns={"value": "time", "num_players": "team_1"}
#     )
#     df_num_players_1["team_2"] = np.nan
#     df_num_players_2 = df_num_players_2.rename(
#         columns={"value": "time", "num_players": "team_2"}
#     )
#     df_num_players_2["team_1"] = np.nan

#     df_exclude = pd.concat([df_num_players_1, df_num_players_2])
#     df_exclude = df_exclude.sort_values("time", ignore_index=True)
#     df_exclude = df_exclude.ffill()

#     mask = df_exclude["time"].shift(-1) != df_exclude["time"]
#     df_exclude = df_exclude[mask]

#     diff = df_exclude["team_1"] != df_exclude["team_2"]
#     missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
#     df_exclude["exclude"] = diff & missing
#     df_exclude = df_exclude.reset_index(drop=True)

#     return df_exclude


# def organize_by_season(seasons, df):
#     """
#     function is designed to process
#     hockey data for multiple seasons and
#     compute advanced statistics
#     (related to Corsi metrics) for
#     each season. The function works with
#     several related DataFrames,
#     filtering, merging, and manipulating
#     the data for each season.
#     """
#     df_orig = df
#     nhl_dfs = []
#     for season in seasons:
#         print(f"Processing season: {season}")
#         df = df_orig.copy()
#         df["game"] = df["game"].query(f"season == {season}")

#         # Debugging: Print game data for the current season
#         print(f"Games for season {season}:")
#         print(df["game"].head())

#         for name in ["game_skater_stats", "game_plays", "game_shifts"]:
#             df[name] = pd.merge(
#                 df[name], df["game"][["game_id"]], on="game_id"
#             ).drop_duplicates()

#             for key, val in df.items():
#                 print(f"{key:>25}: {len(val)}")

#         # Debugging: Print data before filtering game_plays
#         print("game_plays before filtering events:")
#         print(df["game_plays"].head())

#         # cols = ["play_id", "game_id", "team_id_for", "event", "time"]
#         cols = ["play_id", "game_id", "team_id_for", "team_id_against", "event", "time"]
#         events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
#         df["game_plays"] = df["game_plays"].loc[df["game_plays"]["event"].isin(events)]
#         df["game_plays"]["time"] = (
#             df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
#         )
#         df["game_plays"] = df["game_plays"][cols]

#         print(f"reduced game_plays num rows: {len(df['game_plays'])}")
#         print(df["game_plays"].head())  # Print first few rows for debugging

#         # Debugging: Print game_skater_stats and game_shifts before merging
#         print("game_skater_stats before merging with game_shifts:")
#         print(df["game_skater_stats"].head())
#         print("game_shifts before merging with game_skater_stats:")
#         print(df["game_shifts"].head())

#         df["game_skater_stats"] = pd.merge(
#             df["game_skater_stats"], df["game_shifts"][["game_id"]], on="game_id"
#         ).drop_duplicates(ignore_index=True)

#         print("Merged game_skater_stats:")
#         print(df["game_skater_stats"].head())

#         df_corsi = df["game_skater_stats"].sort_values(
#             ["game_id", "player_id"], ignore_index=True
#         )[["game_id", "player_id", "team_id"]]

#         print(f"df_corsi for season {season}:")
#         print(df_corsi.head())

#         print(f"Calling create_corsi_stats for season: {season}")
#         nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])
#         print(f"Completed create_corsi_stats for season: {season}")

#     return nhl_dfs


# def create_corsi_stats(df_corsi, df):
#     """
#     function calculates Corsi statistics
#     for individual players across different
#     games using a DataFrame (df_corsi)
#     that contains player and game information.
#     Corsi is an advanced hockey statistic used
#     to measure shot attempts and is often used
#     as a proxy for puck possession.
#     """
#     print("Entered create_corsi_stats")
#     df_corsi[["corsi_for", "corsi_against", "corsi"]] = np.nan

#     game_id_prev = None
#     shifts_game, plays_game = None, None
#     t1 = perf_counter()

#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 1000 == 0:
#             print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         if game_id != game_id_prev:
#             game_id_prev = game_id
#             shifts_game = df["game_shifts"].query(f"game_id == {game_id}")
#             plays_game = df["game_plays"].query(f"game_id == {game_id}")

#             gss = df["game_skater_stats"].query(f"game_id == {game_id}")
#             if 0 in [len(shifts_game), len(gss)]:
#                 print(f"game_id: {game_id}")
#                 print("Empty DF before Merge.")
#                 continue  # Skip to the next iteration if there's an empty DataFrame

#             df_num_players = get_penalty_exclude_times(shifts_game, gss).reset_index(
#                 drop=True
#             )
#             idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
#             idx[idx < 0] = 0
#             mask = df_num_players["exclude"][idx]
#             mask = mask.reset_index(drop=True).to_numpy()
#             plays_game = plays_game.loc[~mask]

#         shifts_player = shifts_game.query(f"player_id == {player_id}")
#         mask = (
#             shifts_player["shift_start"].searchsorted(plays_game["time"])
#             - shifts_player["shift_end"].searchsorted(plays_game["time"])
#         ).astype(bool)

#         plays_player = plays_game[mask]

#         corsi_for = (plays_player["team_id_for"] == team_id).sum()
#         #corsi_against = len(plays_player) - corsi_for
#         corsi_against = (plays_player["team_id_for"] != team_id).sum()
#         corsi = corsi_for - corsi_against
#         df_corsi.iloc[i, 3:] = [corsi_for, corsi_against, corsi]

#     df_corsi["CF_Percent"] = df_corsi["corsi_for"] / (
#         df_corsi["corsi_for"] + df_corsi["corsi_against"]
#     )

#     print(df_corsi.head())  # Print first few rows of df_corsi for debugging

#     if game_id_prev is not None:
#         print(f"Processed Corsi stats for game {game_id_prev}")

#     return df_corsi


# def write_csv(dfs):
#     """
#     function is responsible for saving
#     a list of DataFrames to CSV files,
#     with each file named according to a
#     season identifier.
#     """
#     relative_directory = "corsi_stats"

#     if not os.path.exists(relative_directory):
#         os.makedirs(relative_directory)

#     for df in dfs:
#         file_path = f"{relative_directory}/corsi_{df[0]}.csv"
#         df[1].to_csv(file_path, index=False)
#         print(f"Written to {file_path}")


# def calculate_and_save_corsi_stats():
#     """
#     function is a high-level function
#     that orchestrates the loading,
#     processing, and saving of hockey
#     Corsi statistics data.
#     """
#     # Get environment variables using the get_env_vars function from load_data.py
#     env_vars = get_env_vars()
#     df_master = load_data(env_vars)
#     print("Data loaded successfully")

#     # Print out the first few rows of each DataFrame to verify data loading
#     for name, df in df_master.items():
#         print(f"{name}:")
#         print(df.head())

#     seasons = [20152016, 20162017, 20172018]
#     nhl_dfs = organize_by_season(seasons, df_master)
#     print("Data organized by season")

#     write_csv(nhl_dfs)
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
import os
from time import perf_counter

import numpy as np
import pandas as pd

from load_data import get_env_vars, load_data

# Set up logging with explicit confirmation of path
# log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing.log"
# log_directory = os.path.dirname(log_file_path)
# if not os.path.exists(log_directory):
#     os.makedirs(log_directory)
#     print(f"Created log directory: {log_directory}")
# else:
#     print(f"Log directory exists: {log_directory}")

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[logging.FileHandler(log_file_path, mode="w"), logging.StreamHandler()],
# )

# # Test to confirm logger output
# logging.info("Logger configured successfully. Test message to ensure logging works.")
# print(f"Logging to file: {log_file_path}")

# Function Definitions


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


def get_penalty_exclude_times(game_shifts, game_skater_stats):
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
    game_shifts = game_shifts.drop(columns=["team_id_y"]).rename(
        columns={"team_id_x": "team_id"}
    )

    # Divide shifts by team
    team_1 = game_shifts.iloc[0]["team_id"]
    mask = game_shifts["team_id"] == team_1
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    # Calculate the number of players on each team and proceed as before
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
    df_exclude = df_exclude.ffill()

    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask]

    # Determine exclusions based on player counts
    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = diff & missing
    df_exclude = df_exclude.reset_index(drop=True)
    return df_exclude

def organize_by_season(seasons, df):
    df_orig, nhl_dfs = df, []
    # game_id = 2015020002

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

    return nhl_dfs

# Save this!!! This is the corsi code for all players.

def calculate_and_save_corsi_stats(df_master, season, game_id = None):
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
    # penalties = game_plays[game_plays["event"] == "Penalty"]
    # for _, row in penalties.iterrows():
    #     logging.info(
    #         f"Penalty Event - Game ID: {row['game_id']}, Time: {row['time']}, Team: {row['team_id_for']}"
    #     )

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
            # logging.info(
            #     f"Player ID: {player_id}, Shift - Game ID: {shift['game_id']}, Start: {shift_start}, End: {shift_end}, "
            #     f"Shift CF: {shift_cf}, Shift CA: {shift_ca}, "
            #     f"Shift Blocked Shots For: {shift_blocked_for}, Shift Blocked Shots Against: {shift_blocked_against}, "
            #     f"Cumulative CF: {player_stats[player_id]['CF']}, Cumulative CA: {player_stats[player_id]['CA']}, "
            #     f"Cumulative Blocked Shots For: {player_stats[player_id]['Blocked For']}, "
            #     f"Cumulative Blocked Shots Against: {player_stats[player_id]['Blocked Against']}"
            # )

    # Log Blocked Shot Events for auditing
    # blocked_shot_events = game_plays[game_plays["event"] == "Blocked Shot"]
    # for index, event in blocked_shot_events.iterrows():
    #     logging.info(
    #         f"Blocked Shot Event - Game ID: {event['game_id']}, Time: {event['time']}, "
    #         f"Team For: {event['team_id_for']}, Team Against: {event['team_id_against']}"
    #     )

def write_csv(dfs):
    """
    function is responsible for saving
    a list of DataFrames to CSV files,
    with each file named according to a
    season identifier.
    """
    relative_directory = "corsi_stats"

    if not os.path.exists(relative_directory):
        os.makedirs(relative_directory)

    for df in dfs:
        file_path = f"{relative_directory}/corsi_{df[0]}.csv"
        df[1].to_csv(file_path, index=False)
        print(f"Written to {file_path}")


# def create_corsi_stats(df_corsi, df):
#     logging.info("Entered create_corsi_stats")

#     # Initialize Corsi statistics columns with zeros for all players
#     df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
#     relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

#     # Ensure the 'time' column is created and available in game_plays
#     if "game_plays" in df:
#         if (
#             "periodTime" in df["game_plays"].columns
#             and "period" in df["game_plays"].columns
#         ):
#             # Calculate 'time' based on period and periodTime
#             df["game_plays"]["time"] = (
#                 df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
#             )
#             logging.info("Calculated 'time' column in game_plays.")
#         else:
#             logging.error("'periodTime' or 'period' column missing in game_plays.")
#             return

#         # Filter game_plays to include only relevant events
#         game_plays = df["game_plays"][df["game_plays"]["event"].isin(relevant_events)]
#         game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])

#         # Verify 'time' column presence after filtering
#         if "time" not in game_plays.columns:
#             logging.error(
#                 "The 'time' column is missing from game_plays after filtering."
#             )
#             return
#         else:
#             logging.info("Verified 'time' column exists in game_plays after filtering.")
#     else:
#         logging.error("'game_plays' DataFrame missing in df.")
#         return

#     game_id_prev = None
#     t1 = perf_counter()

#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 1000 == 0:
#             print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         if game_id != game_id_prev:
#             game_id_prev = game_id

#             # Filter game_shifts and game_plays for this game_id
#             game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
#             plays_game = df["game_plays"].query(f"game_id == {game_id}")

#             # Log the 'time' column in plays_game to verify its presence
#             if "time" not in plays_game.columns:
#                 logging.error(
#                     "'time' column missing in plays_game after filtering by game_id."
#                 )
#                 return
#             else:
#                 logging.info(
#                     f"'time' column verified in plays_game for game_id {game_id}."
#                 )

#             # Continue with the rest of the function as needed, adding further logs as appropriate

#             # Merge team_id into game_shifts and ensure team_id is an integer after merging
#             game_shifts = pd.merge(
#                 game_shifts,
#                 df["game_skater_stats"][["game_id", "player_id", "team_id"]],
#                 on=["game_id", "player_id"],
#                 how="left",
#             )

#             game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
#             game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

#             gss = df["game_skater_stats"].query(f"game_id == {game_id}")
#             df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(
#                 drop=True
#             )

#             idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
#             idx[idx < 0] = 0
#             mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
#             plays_game = plays_game.loc[~mask]

#     for _, event in plays_game.iterrows():
#         event_time = event["time"]
#         team_for = event["team_id_for"]
#         team_against = event["team_id_against"]

#         # Find all players on ice for both teams at this event time
#         players_on_ice = game_shifts[
#             (game_shifts["shift_start"] <= event_time)
#             & (game_shifts["shift_end"] >= event_time)
#         ]

#         # Separate players by team
#         players_for_team = players_on_ice[players_on_ice["team_id"] == team_for]
#         players_against_team = players_on_ice[players_on_ice["team_id"] == team_against]

#         # # Log the number of players on each team at this event
#         # logging.info(
#         #     f"Event: {event['event']} at time {event_time} - "
#         #     f"Players for team {team_for} on ice: {len(players_for_team)}, "
#         #     f"Players against team {team_against} on ice: {len(players_against_team)}"
#         # )

#         # # Log player IDs for detailed tracking
#         # logging.info(
#         #     f"Player IDs for team {team_for} on ice: {players_for_team['player_id'].tolist()}"
#         # )
#         # logging.info(
#         #     f"Player IDs for team {team_against} on ice: {players_against_team['player_id'].tolist()}"
#         # )

#         # Corsi calculations and log updates
#         if event["event"] in ["Shot", "Goal", "Missed Shot"]:
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"
#             ] += 1
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_against_team["player_id"]),
#                 "corsi_against",
#             ] += 1

#             # logging.info(
#             #     f"{event['event']} - CF updated for players on team {team_for} "
#             #     f"and CA updated for players on team {team_against}"
#             # )

#         elif event["event"] == "Blocked Shot":
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_for_team["player_id"]),
#                 "corsi_against",
#             ] += 1
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_against_team["player_id"]),
#                 "corsi_for",
#             ] += 1

#             # logging.info(
#             #     f"Blocked Shot - CA updated for players on team {team_for} "
#             #     f"and CF updated for players on team {team_against}"
#             # )

#     # Final log summary
#     logging.info("Corsi calculations complete. Summary of first few rows:")
#     logging.info(df_corsi.head(5))

#     seasons = [20152016, 20162017, 20172018]
#     nhl_dfs = organize_by_season(seasons, df_master)
#     print("Data organized by season")

#     write_csv(nhl_dfs)

# def create_corsi_stats(df_corsi, df, season):
#     logging.info(f"Entered create_corsi_stats for season {season}")

#     # Initialize Corsi statistics columns with zeros for all players
#     df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
#     relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

#     # Ensure the 'time' column is created and available in game_plays
#     if "game_plays" in df:
#         if "periodTime" in df["game_plays"].columns and "period" in df["game_plays"].columns:
#             df["game_plays"]["time"] = df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
#             logging.info("Calculated 'time' column in game_plays.")
#         else:
#             logging.error("'periodTime' or 'period' column missing in game_plays.")
#             return

#         # Filter game_plays to include only relevant events
#         game_plays = df["game_plays"][df["game_plays"]["event"].isin(relevant_events)]
#         game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])

#         if "time" not in game_plays.columns:
#             logging.error("The 'time' column is missing from game_plays after filtering.")
#             return
#         else:
#             logging.info("Verified 'time' column exists in game_plays after filtering.")
#     else:
#         logging.error("'game_plays' DataFrame missing in df.")
#         return

#     game_id_prev = None
#     t1 = perf_counter()

#     # Process each row in df_corsi for Corsi calculations
#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 1000 == 0:
#             print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         if game_id != game_id_prev:
#             game_id_prev = game_id

#             # Filter game_shifts and game_plays for this game_id
#             game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
#             plays_game = df["game_plays"].query(f"game_id == {game_id}")

#             if "time" not in plays_game.columns:
#                 logging.error("'time' column missing in plays_game after filtering by game_id.")
#                 return
#             else:
#                 logging.info(f"'time' column verified in plays_game for game_id {game_id}.")

#             game_shifts = pd.merge(
#                 game_shifts,
#                 df["game_skater_stats"][["game_id", "player_id", "team_id"]],
#                 on=["game_id", "player_id"],
#                 how="left",
#             )

#             game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
#             game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

#             gss = df["game_skater_stats"].query(f"game_id == {game_id}")
#             df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(drop=True)

#             idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
#             idx[idx < 0] = 0
#             mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
#             plays_game = plays_game.loc[~mask]

#     # Process each event in plays_game for Corsi calculations
#     for _, event in plays_game.iterrows():
#         event_time = event["time"]
#         team_for = event["team_id_for"]
#         team_against = event["team_id_against"]

#         players_on_ice = game_shifts[
#             (game_shifts["shift_start"] <= event_time) & (game_shifts["shift_end"] >= event_time)
#         ]

#         players_for_team = players_on_ice[players_on_ice["team_id"] == team_for]
#         players_against_team = players_on_ice[players_on_ice["team_id"] == team_against]

#         if event["event"] in ["Shot", "Goal", "Missed Shot"]:
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"
#             ] += 1
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_against"
#             ] += 1
#         elif event["event"] == "Blocked Shot":
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_against"
#             ] += 1
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_for"
#             ] += 1

#     logging.info(f"Corsi calculations complete for season {season}. Summary of first few rows:")
#     logging.info(df_corsi.head(5))

#     # Write season-specific results to a CSV
#     write_csv([(season, df_corsi)])


# def main():
#     env_vars = get_env_vars()
#     df_master = load_data(env_vars)
#     seasons = [20152016, 20162017, 20172018]

#     for season in seasons:
#         logging.info(f"Starting Corsi calculations for season: {season}")
#         calculate_and_save_corsi_stats(df_master, season)
#         logging.info(f"Completed Corsi calculations for season: {season}")

def create_corsi_stats(df_corsi, df, season):
    logging.info(f"Entered create_corsi_stats for season {season}")

    # Initialize Corsi statistics columns with zeros for all players
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

    # Ensure the 'time' column is created in game_plays
    if "game_plays" in df:
        if "periodTime" in df["game_plays"].columns and "period" in df["game_plays"].columns:
            df["game_plays"]["time"] = df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
            logging.info("Calculated 'time' column in game_plays.")
        else:
            logging.error("'periodTime' or 'period' column missing in game_plays.")
            return

        # Filter game_plays to include only relevant events
        game_plays = df["game_plays"][df["game_plays"]["event"].isin(relevant_events)]
        game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])

        if "time" not in game_plays.columns:
            logging.error("The 'time' column is missing from game_plays after filtering.")
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

        if i % 1000 == 0:
            print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

        if pd.isna(game_id):
            print(f"Skipping row with NaN game_id: {row}")
            continue

        if game_id != game_id_prev:
            game_id_prev = game_id

            # Filter game_shifts and game_plays for this game_id
            game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = game_plays.query(f"game_id == {game_id}")  # Use the already filtered game_plays

            if game_shifts.empty:
                logging.warning(f"Warning: game_shifts is empty for game_id {game_id}")
                continue

            if "time" not in plays_game.columns:
                logging.error("'time' column missing in plays_game after filtering by game_id.")
                return
            else:
                logging.info(f"'time' column verified in plays_game for game_id {game_id}.")

            game_shifts = pd.merge(
                game_shifts,
                df["game_skater_stats"][["game_id", "player_id", "team_id"]],
                on=["game_id", "player_id"],
                how="left",
            )

            game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
            game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(drop=True)

            if "time" not in df_num_players.columns:
                logging.error("'time' column missing in df_num_players.")
                return

            idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
            idx[idx < 0] = 0
            mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
            plays_game = plays_game.loc[~mask]

    # Continue processing events and finalize Corsi calculations
    logging.info(f"Corsi calculations complete for season {season}. Summary of first few rows:")
    logging.info(df_corsi.head(5))

    # Write season-specific results to a CSV
    write_csv([(season, df_corsi)])


def main():
    env_vars = get_env_vars()
    df_master = load_data(env_vars)
    seasons = [20152016, 20162017, 20172018]

    for season in seasons:
        df_season = {key: df[df["season"] == season] if "season" in df.columns else df for key, df in df_master.items()}
        df_corsi = df_season["game_skater_stats"].copy()  # Select only the required data for Corsi calculations

        logging.info(f"Starting Corsi calculations for season: {season}")
        create_corsi_stats(df_corsi, df_season, season)
        logging.info(f"Completed Corsi calculations for season: {season}")



if __name__ == "__main__":
    main()
