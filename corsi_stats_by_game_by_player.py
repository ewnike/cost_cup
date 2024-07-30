"""
July 4, 2024
Current edition of corsi code.
Eric Winiecke
"""

import os
from time import perf_counter

import numpy as np
import pandas as pd


# Reads in Kaggle .csv file of NHL stats and performs initial cleaning
def load_data():
    names = ["game_skater_stats", "game_plays", "game_shifts", "game"]
    t2 = perf_counter()
    df = {}

    print("load")
    for name in names:
        # df[name] = pd.read_csv(f"/kaggle/input/nhl-game-data/{name}.csv").drop_duplicates(ignore_index=True)#C:\Users\eric\Documents\cost_of_cup\Kaggle_Big_stats
        # print(name)
        df[name] = pd.read_csv(
            f"C:\\Users\\eric\\Documents\\cost_of_cup\\Kaggle_Big_stats\\{name}.csv"
        ).drop_duplicates(ignore_index=True)
        t1, t2 = t2, perf_counter()
        print(f"{name:>25}: {t2 - t1:.4g} sec, {len(df[name])} rows")
        # return a dict of df
    return df


def get_num_players(shift_df):
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

    # parameters: dataframes that have been filtered down to the game of interest


# requires: get_num_players function


def get_penalty_exclude_times(game_plays, game_shifts, game_skater_stats):
    # add team_id to shifts

    game_shifts = pd.merge(
        game_shifts, game_skater_stats[["player_id", "team_id"]], on="player_id"
    )
    if len(game_shifts) == 0:
        print("FIRE in the HOUSE")
        print(game_shifts)

    team_1 = game_shifts.loc[0, "team_id"]
    mask = game_shifts["team_id"] == team_1

    # separate team shift tables
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    # get time tables for player count for each team
    df_num_players_1 = get_num_players(shifts_1)
    df_num_players_2 = get_num_players(shifts_2)

    # synthesize into joint table
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

    # make boolean "exclude" column to say whether times after should be included
    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = diff & missing
    df_exclude = df_exclude.reset_index(drop=True)

    return df_exclude

    # Breaks NHL dataframe down into individual seasons


def organize_by_season(seasons, df):
    df_orig = df
    nhl_dfs = []
    for season in seasons:
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season}")
        # filter games to just 20182019 season
        # when we call df, we are actually calling the keys in the dict of df and this is why we can now call df[]as opposed to df_game....
        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            # do an inner merge to reduce the number of rows...keeping only the rows where game and game_id match ....
            df[name] = pd.merge(
                df[name], df["game"][["game_id"]], on="game_id"
            ).drop_duplicates()

            for key, val in df.items():
                print(f"{key:>25}: {len(val)}")
        # reduce df['game_plays'] df in advance
        cols = ["play_id", "game_id", "team_id_for", "event", "time"]
        events = [
            "Shot",
            "Blocked Shot",
            "Missed Shot",
            "Goal",
        ]  # Add penalty event ...Major vs Minor  and goal within time frame of minor but depends on logic of 4 v 4 or 5 v 4 etc....
        # using .loc here as a mask
        df["game_plays"] = df["game_plays"].loc[df["game_plays"]["event"].isin(events)]
        # defining "time" col
        df["game_plays"]["time"] = (
            df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
        )
        df["game_plays"] = df["game_plays"][cols]

        print(f"reduced game_plays num rows: {len(df['game_plays'])}")
        # filter down to the game_id
        df["game_skater_stats"] = pd.merge(
            df["game_skater_stats"], df["game_shifts"][["game_id"]], on="game_id"
        ).drop_duplicates(ignore_index=True)

        # initialize corsi df
        # sort all rows by game_id and on ties defer to player_id... everything with the same game_id will be grouped together
        df_corsi = df["game_skater_stats"].sort_values(
            ["game_id", "player_id"], ignore_index=True
        )[["game_id", "player_id", "team_id"]]

        nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])

    return nhl_dfs


# Takes a list of pandas dataframes, calculates corsi statistics and adds them to dataframes
def create_corsi_stats(df_corsi, df):
    df_corsi[["CF", "CA", "C"]] = np.nan

    game_id_prev = None
    shifts_game, plays_game = None, None
    t1 = perf_counter()
    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row.iloc[:3]
        # game_id, player_id, team_id = row.loc[['game_id', 'player_id', 'team_id']]

        if i % 1000 == 0:
            print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")
        if game_id != game_id_prev:
            game_id_prev = game_id
            shifts_game = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = df["game_plays"].query(f"game_id == {game_id}")

            # added this block of code (2lines)
            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            if 0 in [len(shifts_game), len(gss)]:
                print(f"game_id: {game_id}")
                print("Empty DF before Merge.")

            df_num_players = get_penalty_exclude_times(
                plays_game, shifts_game, gss
            ).reset_index(drop=True)
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
        # commented out following line and added 2 after
        # plays_player = plays_game[mask]
        plays_player = plays_game[mask]  # .reset_index(drop = True)

        # mask = df_num_players['exclude'][df_num_players['time'].searchsorted(plays_game['time'])-1]
        # mask = mask.reset_index(drop = True)

        # plays_game = plays_game[~mask]
        CF = (plays_player["team_id_for"] == team_id).sum()
        CA = len(plays_player) - CF
        C = CF - CA
        df_corsi.iloc[i, 3:] = [CF, CA, C]

    df_corsi["CF_Percent"] = df_corsi["CF"] / (df_corsi["CF"] + df_corsi["CA"])

    return df_corsi


def write_csv(dfs):
    for df in dfs:
        df[1].to_csv(
            f"C:\\Users\\eric\\Documents\\cost_of_cup\\corsi_vals_II\\corsi_{df[0]}.csv"
        )
        # C:\Users\eric\Documents\cost_of_cup\corsi_vals_II
        # C:\\Users\\eric\\Documents\\cost_of_cup\\Kaggle_Big_stats\\{name


def main():
    df_master = load_data()
    seasons = [20152016, 20162017, 20172018]
    # seasons = [20152016]
    nhl_dfs = organize_by_season(seasons, df_master)
    write_csv(nhl_dfs)


if __name__ == "__main__":
    main()
