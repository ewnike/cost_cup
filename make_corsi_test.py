import numpy as np
import pandas as pd
import logging
import os
from time import perf_counter
from load_data import get_env_vars, load_data

# Set up streamlined logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

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
        logging.warning("game_shifts is empty in get_penalty_exclude_times")
        return pd.DataFrame()

    # Merge team_id into game_shifts
    game_shifts = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left"
    )
    game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(
        columns={"team_id_x": "team_id"}
    )

    # Divide shifts by team and calculate num_players
    team_1 = game_shifts.iloc[0]["team_id"]
    mask = game_shifts["team_id"] == team_1
    df_num_players_1 = get_num_players(game_shifts[mask])
    df_num_players_2 = get_num_players(game_shifts[~mask])

    df_num_players_1 = df_num_players_1.rename(columns={"value": "time", "num_players": "team_1"})
    df_num_players_2 = df_num_players_2.rename(columns={"value": "time", "num_players": "team_2"})

    df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values("time", ignore_index=True).ffill()
    df_exclude = df_exclude[df_exclude["time"].shift(-1) != df_exclude["time"]]

    # Exclusion logic
    df_exclude["exclude"] = ((df_exclude["team_1"] != df_exclude["team_2"]) |
                             (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5))
    return df_exclude.reset_index(drop=True)



# def create_corsi_stats(df_corsi, df_game):
#     # Construct an empty DataFrame to return if needed
#     empty_corsi_df = pd.DataFrame(columns=["game_id", "player_id", "team_id", "corsi_for", "corsi_against", "corsi", "CF_Percent"])

#     if df_corsi.empty:
#         logging.warning("Skipping game: df_corsi is empty.")
#         return empty_corsi_df  # Return an empty DataFrame

#     if df_game["game_shifts"].empty:
#         game_id = df_corsi['game_id'].iloc[0] if not df_corsi.empty else "unknown"
#         logging.warning(f"Skipping game {game_id}: game_shifts is empty.")
#         return empty_corsi_df  # Return an empty DataFrame

#     # Continue with the rest of the function...

#     # Check if essential dataframes are empty
#     if df_game["game_shifts"].empty:
#         logging.warning(f"Skipping game {df_corsi['game_id'].iloc[0]}: game_shifts is empty.")
#         return pd.DataFrame()  # Return an empty DataFrame to skip this game

#     if df_game["game_plays"].empty:
#         logging.warning(f"Skipping game {df_corsi['game_id'].iloc[0]}: game_plays is empty.")
#         return pd.DataFrame()  # Return an empty DataFrame to skip this game

#     # Continue with existing logic if the data is present
#     game_plays = df_game["game_plays"].reset_index(drop=True).copy()
#     game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
#     logging.info("Calculated 'time' column in game_plays.")
#     # Initialize game_plays and calculate 'time' column
#     game_plays = df_game["game_plays"].reset_index(drop=True).copy()
#     game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
#     logging.info("Calculated 'time' column in game_plays.")
#     df_corsi[["corsi_for", "corsi_against"]] = 0

#     # Initialize game_shifts and ensure it is defined
#     game_shifts = df_game["game_shifts"]
#     game_skater_stats = df_game["game_skater_stats"]

#     # Get exclusion times from penalty events
#     df_num_players = get_penalty_exclude_times(game_shifts, game_skater_stats).reset_index(drop=True)

#     # Ensure 'time' column exists in both dataframes and align indices
#     idx = df_num_players["time"].searchsorted(game_plays["time"]) - 1
#     idx[idx < 0] = 0

#     # Create a mask from the exclusion column
#     mask = df_num_players["exclude"].iloc[idx].reset_index(drop=True).to_numpy()

#     # Apply the mask to filter out plays during penalty exclusion times
#     plays_game = game_plays.loc[~mask]
#     t1 = perf_counter()

#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row["game_id"], row["player_id"], row["team_id"]
#         relevant_shifts = game_shifts[(game_shifts["game_id"] == game_id) & (game_shifts["player_id"] == player_id)]

#         for _, shift in relevant_shifts.iterrows():
#             shift_start, shift_end = shift["shift_start"], shift["shift_end"]
#             plays_during_shift = plays_game[(plays_game["time"] >= shift_start) & (plays_game["time"] <= shift_end)]

#             for _, event in plays_during_shift.iterrows():
#                 if event["event"] in ["Shot", "Goal", "Missed Shot"]:
#                     if event["team_id_for"] == team_id:
#                         df_corsi.at[i, "corsi_for"] += 1
#                     elif event["team_id_against"] == team_id:
#                         df_corsi.at[i, "corsi_against"] += 1
#                 elif event["event"] == "Blocked Shot":
#                     if event["team_id_for"] == team_id:
#                         df_corsi.at[i, "corsi_against"] += 1
#                     elif event["team_id_against"] == team_id:
#                         df_corsi.at[i, "corsi_for"] += 1

#         if i % 100 == 0:
#             print(f"Processed {i}/{len(df_corsi)} players, {perf_counter() - t1:.2f}s elapsed.")

#     # Final calculations
#     df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
#     df_corsi["CF_Percent"] = (df_corsi["corsi_for"] / (df_corsi["corsi_for"] + df_corsi["corsi_against"])).fillna(0).round(4) * 100
#     return df_corsi[["game_id", "player_id", "team_id", "corsi_for", "corsi_against", "corsi", "CF_Percent"]]

def create_corsi_stats(df_corsi, df_game):
    # Construct an empty DataFrame to return if needed
    empty_corsi_df = pd.DataFrame(columns=["game_id", "player_id", "team_id", "corsi_for", "corsi_against", "corsi", "CF_Percent"])

    # Check if df_corsi is empty
    if df_corsi.empty:
        logging.warning("Skipping game: df_corsi is empty.")
        return empty_corsi_df

    # Check if game_shifts is empty
    if df_game["game_shifts"].empty:
        game_id = df_corsi['game_id'].iloc[0] if not df_corsi.empty else "unknown"
        logging.warning(f"Skipping game {game_id}: game_shifts is empty.")
        return empty_corsi_df

    # Prepare game_plays with the 'time' column
    game_plays = df_game["game_plays"].reset_index(drop=True).copy()
    if "time" not in game_plays.columns:
        game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
        logging.info("Calculated 'time' column in game_plays.")

    # Merge team_id into game_shifts
    game_shifts = pd.merge(
        df_game["game_shifts"],
        df_game["game_skater_stats"][["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left"
    )

    # Ensure 'shift_start' and 'shift_end' are integers for comparison
    game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
    game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

    # Get penalty exclude times
    df_num_players = get_penalty_exclude_times(game_shifts, df_game["game_skater_stats"]).reset_index(drop=True)

    # Ensure there are 'time' and 'exclude' columns in df_num_players
    if "time" not in df_num_players or "exclude" not in df_num_players:
        logging.error("Missing 'time' or 'exclude' columns in df_num_players.")
        return empty_corsi_df

    # Apply the mask to exclude plays during penalties
    idx = df_num_players["time"].searchsorted(game_plays["time"]) - 1
    idx[idx < 0] = 0
    mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
    plays_game = game_plays.loc[~mask]

    # Initialize columns for Corsi statistics
    df_corsi[["corsi_for", "corsi_against"]] = 0

    t1 = perf_counter()

    # Iterate through each player to calculate their Corsi stats
    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row["game_id"], row["player_id"], row["team_id"]
        player_shifts = game_shifts[(game_shifts["game_id"] == game_id) & (game_shifts["player_id"] == player_id)]

        for _, shift in player_shifts.iterrows():
            shift_start, shift_end = shift["shift_start"], shift["shift_end"]
            plays_during_shift = plays_game[(plays_game["time"] >= shift_start) & (plays_game["time"] <= shift_end)]

            # Iterate over each play during the shift and calculate Corsi
            for _, play in plays_during_shift.iterrows():
                if play["event"] in ["Shot", "Goal", "Missed Shot"]:
                    if play["team_id_for"] == team_id:
                        df_corsi.at[i, "corsi_for"] += 1
                    elif play["team_id_against"] == team_id:
                        df_corsi.at[i, "corsi_against"] += 1
                elif play["event"] == "Blocked Shot":
                    if play["team_id_for"] == team_id:
                        df_corsi.at[i, "corsi_against"] += 1
                    elif play["team_id_against"] == team_id:
                        df_corsi.at[i, "corsi_for"] += 1

        if i % 100 == 0:
            logging.info(f"Processed {i}/{len(df_corsi)} players, {perf_counter() - t1:.2f}s elapsed.")

    # Final calculations for 'corsi' and 'CF_Percent'
    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]
    df_corsi["CF_Percent"] = (df_corsi["corsi_for"] / (df_corsi["corsi_for"] + df_corsi["corsi_against"])).fillna(0).round(4) * 100

    logging.info("Corsi calculations complete. Summary of first few rows:")
    logging.info(df_corsi.head(5))

    return df_corsi[["game_id", "player_id", "team_id", "corsi_for", "corsi_against", "corsi", "CF_Percent"]]



def organize_by_season(seasons, df_master):
    organized_data = []
    for season in seasons:
        df_season = {key: df[df["season"] == season] if "season" in df.columns else df for key, df in df_master.items()}
        unique_game_ids = df_season["game"]["game_id"].unique()
        season_data = []

        for game_id in unique_game_ids:
            df_game = {key: df[df["game_id"] == game_id] for key, df in df_season.items()}
            df_corsi = df_game["game_skater_stats"].copy()
            print(f"Calculating Corsi for game {game_id} in season {season}...")
            corsi_stats = create_corsi_stats(df_corsi, df_game)
            season_data.append(corsi_stats)

        season_df = pd.concat(season_data, ignore_index=True)
        organized_data.append((season, season_df))
    return organized_data
# def organize_by_season(seasons, df):
#     df_orig, nhl_dfs = df, []
#     game_id = 2015020002

#     for season in seasons:
#         df = df_orig.copy()
#         df["game"] = df["game"].query(f"season == {season} and game_id == {game_id}")
#         if df["game"].empty:
#             logging.warning(f"Game ID {game_id} not found in season {season}.")
#             continue

#         for name in ["game_skater_stats", "game_plays", "game_shifts"]:
#             df[name] = pd.merge(
#                 df[name][df[name]["game_id"] == game_id],
#                 df["game"][["game_id"]],
#                 on="game_id",
#             ).drop_duplicates()

#         df_corsi = df["game_skater_stats"].sort_values(
#             ["game_id", "player_id"], ignore_index=True
#         )[["game_id", "player_id", "team_id"]]
#         nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])

#     return nhl_dfs

def write_csv(organized_data):
    output_dir = "corsi_stats"
    os.makedirs(output_dir, exist_ok=True)
    for season, df in organized_data:
        file_path = f"{output_dir}/corsi_{season}.csv"
        df.to_csv(file_path, index=False)
        print(f"Saved Corsi data for season {season} to {file_path}")

def calculate_and_save_corsi_stats():
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    seasons = [20152016, 20162017, 20172018]
    organized_data = organize_by_season(seasons, df_master)
    write_csv(organized_data)



def main():
    calculate_and_save_corsi_stats()

if __name__ == "__main__":
    main()
