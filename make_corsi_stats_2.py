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
log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing.log"
log_directory = os.path.dirname(log_file_path)
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    print(f"Created log directory: {log_directory}")
else:
    print(f"Log directory exists: {log_directory}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path, mode="w"), logging.StreamHandler()],
)

# Test to confirm logger output
logging.info("Logger configured successfully. Test message to ensure logging works.")
print(f"Logging to file: {log_file_path}")

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
    game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(columns={"team_id_x": "team_id"})

    # Divide shifts by team
    team_1 = game_shifts.iloc[0]["team_id"]
    mask = game_shifts["team_id"] == team_1
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    # Calculate the number of players on each team and proceed as before
    df_num_players_1 = get_num_players(shifts_1)
    df_num_players_2 = get_num_players(shifts_2)

    # Rename and merge the player counts for each team
    df_num_players_1 = df_num_players_1.rename(columns={"value": "time", "num_players": "team_1"})
    df_num_players_2 = df_num_players_2.rename(columns={"value": "time", "num_players": "team_2"})

    df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values("time", ignore_index=True)
    df_exclude = df_exclude.ffill()

    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask]

    # Determine exclusions based on player counts
    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = diff & missing
    df_exclude = df_exclude.reset_index(drop=True)

    return df_exclude


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

def organize_by_season(seasons, df):
    df_orig, nhl_dfs = df, []
    #game_id = 2015020002

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




# Ensure logging only logs minimal messages to the console for progress updates
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def create_corsi_stats(df_corsi, game_plays, game_shifts, game_skater_stats):
    # Ensure 'time' column in game_plays to avoid SettingWithCopyWarning
    game_plays = game_plays.copy()
    game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
    logging.info("Calculated 'time' column in game_plays.")

    # Initialize Corsi statistics columns
    df_corsi[["corsi_for", "corsi_against"]] = 0

    # Get penalty exclusion times
    exclusion_times = get_penalty_exclude_times(game_shifts, game_skater_stats)["time"]


    # Use searchsorted to exclude plays during penalty times
    idx = np.searchsorted(exclusion_times, game_plays["time"].values)
    mask = (idx < len(exclusion_times)) & (game_plays["time"].values == exclusion_times[idx])
    mask = pd.Series(mask, index=game_plays.index)
    plays_game = game_plays.loc[~mask]

    # Start tracking processing time
    t1 = perf_counter()

    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row["game_id"], row["player_id"], row["team_id"]

        if i % 1000 == 0:
            print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

        # Skip if game_id is NaN
        if pd.isna(game_id):
            print(f"Skipping row with NaN game_id: {row}")
            continue

        # Filter relevant plays within the game for the current game_id
        relevant_game_shifts = game_shifts[game_shifts["game_id"] == game_id]
        relevant_plays = plays_game[plays_game["game_id"] == game_id]

        for _, event in relevant_plays.iterrows():
            event_time = event["time"]
            team_for = event["team_id_for"]
            team_against = event["team_id_against"]

            # Find players on the ice during this event
            players_on_ice = relevant_game_shifts[
                (relevant_game_shifts["shift_start"] <= event_time) & 
                (relevant_game_shifts["shift_end"] >= event_time)
            ]

            players_for_team = players_on_ice[players_on_ice["team_id"] == team_for]
            players_against_team = players_on_ice[players_on_ice["team_id"] == team_against]

            if event["event"] in ["Shot", "Goal", "Missed Shot"]:
                df_corsi.loc[df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"] += 1
                df_corsi.loc[df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_against"] += 1
            elif event["event"] == "Blocked Shot":
                df_corsi.loc[df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_against"] += 1
                df_corsi.loc[df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_for"] += 1

    # Finalize Corsi calculation
    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
    df_corsi["CF_Percent"] = (df_corsi["corsi_for"] / (df_corsi["corsi_for"] + df_corsi["corsi_against"])).fillna(0).round(4) * 100

    return df_corsi[["game_id", "player_id", "team_id", "corsi_for", "corsi_against", "corsi", "CF_Percent"]]

def main():
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    seasons = [20152016, 20162017, 20172018]
    for season in seasons:
        df_season = {key: df[df["season"] == season] if "season" in df.columns else df for key, df in df_master.items()}
        if "game" not in df_season or df_season["game"].empty:
            print(f"No game data found for season {season}. Skipping.")
            continue

        season_corsi_stats = []
        unique_game_ids = df_season["game"]["game_id"].unique()
        print(f"Processing {len(unique_game_ids)} games for season {season}...")

        for game_id in unique_game_ids:
            df_game = {key: df[df["game_id"] == game_id] for key, df in df_season.items()}
            df_corsi = df_game["game_skater_stats"].copy()

            print(f"Calculating Corsi for game {game_id} in season {season}...")
            corsi_stats = create_corsi_stats(df_corsi, df_game["game_plays"], df_game["game_shifts"], df_game["game_skater_stats"])
            season_corsi_stats.append(corsi_stats)

        season_df = pd.concat(season_corsi_stats, ignore_index=True)
        output_dir = "corsi_stats"
        os.makedirs(output_dir, exist_ok=True)
        season_file_path = f"{output_dir}/corsi_{season}.csv"
        season_df.to_csv(season_file_path, index=False)
        print(f"Written to {season_file_path}")



if __name__ == "__main__":
    main()





