"""
July 4, 2024
Current edition of corsi code.
Eric Winiecke
"""

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
    """
    The get_penalty_exclude_times function is
    designed to identify periods during a hockey
    game where there is a difference in the number
    of players on the ice between two teams,
    particularly focusing on when a team has fewer
    than five players (indicating a penalty situation).
    The function processes data on player shifts and
    game stats to determine these periods.
    """
    game_shifts = pd.merge(
        game_shifts, game_skater_stats[["player_id", "team_id"]], on="player_id"
    )
    if len(game_shifts) == 0:
        print("FIRE in the HOUSE")
        print(game_shifts)

    team_1 = game_shifts.loc[0, "team_id"]
    mask = game_shifts["team_id"] == team_1

    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    df_num_players_1 = get_num_players(shifts_1)
    df_num_players_2 = get_num_players(shifts_2)

    df_num_players_1 = df_num_players_1.rename(
        columns={"value": "time", "num_players": "team_1"}
    )
    df_num_players_1["team_2"] = np.nan
    df_num_players_2 = df_num_players_2.rename(
        columns={"value": "time", "num_players": "team_2"}
    )
    df_num_players_2["team_1"] = np.nan

    df_exclude = pd.concat([df_num_players_1, df_num_players_2])
    df_exclude = df_exclude.sort_values("time", ignore_index=True)
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
    for season in seasons:
        print(f"Processing season: {season}")
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season}")

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

        cols = ["play_id", "game_id", "team_id_for", "event", "time"]
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


def create_corsi_stats(df_corsi, df):
    """
    function calculates Corsi statistics
    for individual players across different
    games using a DataFrame (df_corsi)
    that contains player and game information.
    Corsi is an advanced hockey statistic used
    to measure shot attempts and is often used
    as a proxy for puck possession.
    """
    print("Entered create_corsi_stats")
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = np.nan

    game_id_prev = None
    shifts_game, plays_game = None, None
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
            shifts_game = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = df["game_plays"].query(f"game_id == {game_id}")

            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            if 0 in [len(shifts_game), len(gss)]:
                print(f"game_id: {game_id}")
                print("Empty DF before Merge.")
                continue  # Skip to the next iteration if there's an empty DataFrame

            df_num_players = get_penalty_exclude_times(shifts_game, gss).reset_index(
                drop=True
            )
            idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
            idx[idx < 0] = 0
            mask = df_num_players["exclude"][idx]
            mask = mask.reset_index(drop=True).to_numpy()
            plays_game = plays_game.loc[~mask]

        shifts_player = shifts_game.query(f"player_id == {player_id}")
        mask = (
            shifts_player["shift_start"].searchsorted(plays_game["time"])
            - shifts_player["shift_end"].searchsorted(plays_game["time"])
        ).astype(bool)

        plays_player = plays_game[mask]

        corsi_for = (plays_player["team_id_for"] == team_id).sum()
        corsi_against = len(plays_player) - corsi_for
        corsi = corsi_for - corsi_against
        df_corsi.iloc[i, 3:] = [corsi_for, corsi_against, corsi]

    df_corsi["CF_Percent"] = df_corsi["corsi_for"] / (
        df_corsi["corsi_for"] + df_corsi["corsi_against"]
    )

    print(df_corsi.head())  # Print first few rows of df_corsi for debugging

    if game_id_prev is not None:
        print(f"Processed Corsi stats for game {game_id_prev}")

    return df_corsi


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


def calculate_and_save_corsi_stats():
    """
    function is a high-level function
    that orchestrates the loading,
    processing, and saving of hockey
    Corsi statistics data.
    """
    # Get environment variables using the get_env_vars function from load_data.py
    env_vars = get_env_vars()
    df_master = load_data(env_vars)
    print("Data loaded successfully")

    # Print out the first few rows of each DataFrame to verify data loading
    for name, df in df_master.items():
        print(f"{name}:")
        print(df.head())

    seasons = [20152016, 20162017, 20172018]
    nhl_dfs = organize_by_season(seasons, df_master)
    print("Data organized by season")

    write_csv(nhl_dfs)


if __name__ == "__main__":
    calculate_and_save_corsi_stats()
