
import os
from time import perf_counter

import numpy as np
import pandas as pd

from load_data import get_env_vars, load_data


def get_num_players(shift_df):
    """keep track of the number of players on the ice
    at specific times. Important for being able to assign the
    correct shift stats per player."""
    shifts_melted = pd.melt(
        shift_df,
        id_vars=["game_id", "player_id"],
        value_vars=["shift_start", "shift_end"],
    )
    shifts_melted = shifts_melted.sort_values("value", ignore_index=True)
    shifts_melted["change"] = (
        2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
    )
    shifts_melted["num_players"] = shifts_melted["change"].cumsum()
    df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()
    mask = df_num_players["num_players"].shift() != df_num_players["num_players"]
    df_num_players = df_num_players.loc[mask].reset_index(drop=True)

    return df_num_players


def get_penalty_exclude_times(game_shifts, game_skater_stats):
    if game_shifts.empty:
        print("Warning: game_shifts is empty in get_penalty_exclude_times")
        return pd.DataFrame()  # Return an empty DataFrame if no shifts are available

    # Merge the `team_id` column from `game_skater_stats` into `game_shifts`
    # After loading game_shifts, add the following to inspect
    print("Checking team_id in game_shifts within get_penalty_exclude_times:")
    print("Columns:", game_shifts.columns)
    print("Sample data in game_shifts:", game_shifts.head())

    if 'team_id' not in game_shifts.columns:
        raise KeyError("'team_id' missing in game_shifts when accessing in get_penalty_exclude_times")

    print("Original game_shifts data:")
    print(game_shifts[game_shifts["game_id"] == 2015020002].head(10))  # Inspect the first few rows for a specific game_id

    # game_shifts = pd.merge(
    #     game_shifts, game_skater_stats[["game_id", "player_id", "team_id"]],
    #     on=["game_id", "player_id"], how="left"
    # )
    game_shifts = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left"
    )
    # Keep the integer `team_id_x` and rename it to `team_id`
    game_shifts = game_shifts.drop(columns=["team_id_y"]).rename(columns={"team_id_x": "team_id"})

    print(game_shifts.head())
    print('*' * 50)
    # Ensure game_shifts is not empty before accessing team_1
    if not game_shifts.empty:
        team_1 = game_shifts.iloc[0]["team_id"]
        print(team_1, '*' * 50)

    else:
        print("Warning: game_shifts is empty for this game_id")
        return pd.DataFrame()  # Return empty DataFrame if no data
    print('*' * 50)
    # Continue with the rest of the function as usual
    mask = game_shifts["team_id"] == team_1
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]
    print('*' * 50)
    # Debugging output
    print("Sample data in shifts_1:", shifts_1.head())
    print("Sample data in shifts_2:", shifts_2.head())
    
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

    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = diff & missing
    df_exclude = df_exclude.reset_index(drop=True)

    return df_exclude


def organize_by_season(seasons, df):
    """
    function is designed to process
    hockey data for multiple seasons and
    compute advanced statistics
    (related to Corsi metrics) for
    each season. The function works with
    several related DataFrames,
    filtering, merging, and manipulating
    the data for each season.
    """
    df_orig = df
    nhl_dfs = []
    game_id = 2015020002


    for season in seasons:
        print(f"Processing season: {season}")
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season}")

        # Check if the specific game_id exists in the 'game' DataFrame
        if game_id not in df["game"]["game_id"].values:
            print(f"Warning: game_id {game_id} not found in season {season} data.")
            continue  # Skip this season if game_id is not found

        print(f"game_id {game_id} exists in season {season} data.")

        # Filter data by game_id only if it exists in the 'game' DataFrame
        df["game"] = df["game"].query(f"game_id == {game_id}")
        print("Filtered 'game' DataFrame:")
        print(df["game"].head())

        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            if name in df:
                df[name] = df[name][df[name]["game_id"] == game_id]
                print(f"Filtered {name} DataFrame:")
                print(df[name].head())

        # Debugging: Print game data for the current season
        print(f"Games for season {season}:")
        print(df["game"].head())

        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            df[name] = pd.merge(
                df[name], df["game"][["game_id"]], on="game_id"
            ).drop_duplicates()

            for key, val in df.items():
                print(f"{key:>25}: {len(val)}")

        # Debugging: Print data before filtering game_plays
        print("game_plays before filtering events:")
        print(df["game_plays"].head())

        # cols = ["play_id", "game_id", "team_id_for", "event", "time"]
        cols = ["play_id", "game_id", "team_id_for", "team_id_against", "event", "time"]
        events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
        df["game_plays"] = df["game_plays"].loc[df["game_plays"]["event"].isin(events)]
        df["game_plays"]["time"] = (
            df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
        )
        df["game_plays"] = df["game_plays"][cols]

        print(f"reduced game_plays num rows: {len(df['game_plays'])}")
        print(df["game_plays"].head())  # Print first few rows for debugging

        # Debugging: Print game_skater_stats and game_shifts before merging
        print("game_skater_stats before merging with game_shifts:")
        print(df["game_skater_stats"].head())

        print("game_shifts before merging with game_skater_stats:")
        print(df["game_shifts"].head())

        df["game_skater_stats"] = pd.merge(
            df["game_skater_stats"], df["game_shifts"][["game_id"]], on="game_id"
        ).drop_duplicates(ignore_index=True)

        print("Merged game_skater_stats:")
        print(df["game_skater_stats"].head())

        df_corsi = df["game_skater_stats"].sort_values(
            ["game_id", "player_id"], ignore_index=True
        )[["game_id", "player_id", "team_id"]]

        print(f"df_corsi for season {season}:")
        print(df_corsi.head())

        print(f"Calling create_corsi_stats for season: {season}")
        nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])
        print(f"Completed create_corsi_stats for season: {season}")

    return nhl_dfs


def calculate_and_save_corsi_stats(game_id=None):
    """
    Function to orchestrate the loading, processing,
    and saving of hockey Corsi statistics data.
    This version filters by a single game_id for debugging.
    """
    # Get environment variables using the get_env_vars function from load_data.py
    env_vars = get_env_vars()
    df_master = load_data(env_vars)
    print("Data loaded successfully")

    # Print out the first few rows of each DataFrame to verify data loading
    for name, df in df_master.items():
        print(f"{name}:")
        print(df.head())

    # Select a specific game_id if provided for focused debugging
    if game_id:
        df_master = {name: df[df['game_id'] == game_id] for name, df in df_master.items()}
        print(f"Data filtered for game_id {game_id}")

    # Use the organize_by_season with only the data filtered by game_id
    seasons = [20152016]  # Use a dummy season or change as necessary
    nhl_dfs = organize_by_season(seasons, df_master)
    print("Data organized by season")

    # Skip writing to CSV to focus on debugging output
    # write_csv(nhl_dfs)







def create_corsi_stats(df_corsi, df):
    print("Entered create_corsi_stats")
    # Initialize Corsi statistics columns with zeros for all players
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

    # Filter game_plays to include only relevant events
    game_plays = df["game_plays"][df["game_plays"]["event"].isin(relevant_events)]

    print("Filtered game_plays (only relevant events):")
    print(game_plays.head())  # Confirm only relevant events are present
    # Drop rows where `team_id_for` or `team_id_against` are NaN after filtering
    game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])
    print("Filtered game_plays without NaNs in team columns:")
    print(game_plays.head())  # Check that NaNs are removed

    game_id_prev = None
    t1 = perf_counter()

    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row.iloc[:3]

        if game_id != game_id_prev:
            game_id_prev = game_id

            # Filter game_shifts and game_plays for this game_id
            game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = df["game_plays"].query(f"game_id == {game_id}")

            # Merge team_id into game_shifts and ensure team_id is an integer after merging
            game_shifts = pd.merge(
                game_shifts,
                df["game_skater_stats"][["game_id", "player_id", "team_id"]], 
                on=["game_id", "player_id"],
                how="left"
            )

            # Debugging output to ensure 'team_id' was merged correctly
            if 'team_id' not in game_shifts.columns:
                raise KeyError("'team_id' is missing in game_shifts after merge")

            print("Columns in game_shifts after merge:", game_shifts.columns)
            print("Sample of game_shifts after merging with team_id:")
            print(game_shifts.head())

            # Cast shift_start and shift_end to integers
            game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
            game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

            # Debugging output to confirm integer casting
            print("Shift times after type casting to integers:")
            print(game_shifts[["shift_start", "shift_end"]].head())

            # Debug: Check if team_id exists
            if 'team_id' not in game_shifts.columns:
                print("Error: 'team_id' column not found in game_shifts after merge")
            else:
                print("team_id column successfully added. Sample data:")
                print(game_shifts[['game_id', 'player_id', 'team_id']].head())
                        # Cast team_id back to integer if there are no nulls, or handle nulls as needed

            game_shifts["team_id"] = game_shifts["team_id"].fillna(0).astype(int)
            game_shifts['shift_end'] = game_shifts['shift_end'].astype(int)

            # Confirm that team_id is now an integer
            print("Columns in game_shifts after ensuring integer team_id:", game_shifts.dtypes)
            print("Sample data from game_shifts after type conversion:")
            print(game_shifts.head())




            # Exit the function if `team_id` is still missing
            if "team_id" not in game_shifts.columns:
                raise ValueError("team_id column missing in game_shifts after merge")

            # Before calling get_penalty_exclude_times, check if team_id is still present
            print("Sample of game_shifts before calling get_penalty_exclude_times:")
            print(game_shifts.head())
            print("Columns in game_shifts before get_penalty_exclude_times:", game_shifts.columns)

            # Additional check for dtype consistency
            print("Data types in game_shifts:")
            print(game_shifts.dtypes)


            # Before calling get_penalty_exclude_times
            print("Sample of game_shifts before calling get_penalty_exclude_times:")
            print(game_shifts.head())
            print("Columns in game_shifts:", game_shifts.columns)
            if game_shifts.empty:
                raise ValueError("game_shifts is unexpectedly empty when calling get_penalty_exclude_times")


            # Proceed to calculate penalty exclusion times
            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(drop=True)

            # Additional Corsi calculations as necessary


            # Further processing for Corsi calculations
            idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
            idx[idx < 0] = 0
            mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
            plays_game = plays_game.loc[~mask]

            # Debugging: Check game_shifts and plays_game content
            print("Merged game_shifts sample:")
            print(game_shifts.head())
            print("Filtered game_plays sample:")
            print(plays_game.head())

    # For each event, find all players on ice and update Corsi stats for them
    for _, event in plays_game.iterrows():
        event_time = event["time"]
        team_for = event["team_id_for"]
        team_against = event["team_id_against"]

        # Find all players on ice for both teams at this event time
        players_on_ice = game_shifts[
            (game_shifts["shift_start"] <= event_time) & 
            (game_shifts["shift_end"] >= event_time)
        ]

        # Debugging output for players_on_ice
        print("players_on_ice columns:", players_on_ice.columns)
        print("players_on_ice sample data:", players_on_ice.head())

        # Separate players by team explicitly using team_for and team_against
        players_for_team = players_on_ice[players_on_ice["team_id"] == team_for]
        players_against_team = players_on_ice[players_on_ice["team_id"] == team_against]

        # Ensure players are assigned to the correct team
        if players_for_team.empty:
            print(f"Warning: No players found on ice for team_for {team_for} at time {event_time}")
        if players_against_team.empty:
            print(f"Warning: No players found on ice for team_against {team_against} at time {event_time}")

        # Debug output to count each event attribution
        print(f"Event: {event['event']} at time {event_time}")
        print(f"Players for team {team_for} on ice: {len(players_for_team)}")
        print(f"Players against team {team_against} on ice: {len(players_against_team)}")

        # Only increment counts once per player
        if event["event"] in ["Shot", "Goal", "Missed Shot"]:
            # Increment CF for players on the team attempting the shot
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"
            ] += 1
            # Increment CA for players on the opposing team
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_against"
            ] += 1
        elif event["event"] == "Blocked Shot":
            # Reverse the logic for Blocked Shots:
            # The defending team gets CA for blocking the shot
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_against"
            ] += 1
            # The shooting team gets CF for attempting the blocked shot
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_for"
            ] += 1

    # Calculate the final Corsi stats
    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
    df_corsi["CF_Percent"] = df_corsi["corsi_for"] / (
        df_corsi["corsi_for"] + df_corsi["corsi_against"]
    )

    print(df_corsi.head(25))  # Print first few rows of df_corsi for debugging

    return df_corsi


if __name__ == "__main__":
    # Pass a specific game_id to calculate_and_save_corsi_stats for single-game debugging
    calculate_and_save_corsi_stats(game_id= 2015020002)
