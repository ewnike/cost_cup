"""
Eric Winiecke
November 9, 2024
Corsi_Utils.py: Utility functions for Corsi calculations and data processing.
"""

import logging
import os
from time import perf_counter

import numpy as np
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from db_utils import get_db_engine
from load_data import get_env_vars, load_data

# Initialize the database engine
engine = get_db_engine()


# Helper Function: Get Number of Players on Ice
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


# Helper Function: Load Number of Players from Database
def load_num_players_from_db(game_id, season):
    table_name = f"num_players_on_ice_{season}"
    query = f"SELECT * FROM {table_name} WHERE game_id = {game_id}"
    try:
        with engine.connect() as connection:
            df_num_players = pd.read_sql(query, connection)
        return df_num_players if not df_num_players.empty else None
    except SQLAlchemyError as e:
        logging.error(f"Error loading num_players_on_ice for game_id {game_id}: {e}")
        return None


def load_exclude_times_from_db(game_id, season):
    """
    Load penalty exclude times for a given game_id and season from the database.
    """
    table_name = f"penalty_exclude_times_{season}"
    query = f"SELECT * FROM {table_name} WHERE game_id = {game_id}"
    try:
        with engine.connect() as connection:
            df_exclude_times = pd.read_sql(query, connection)
        return df_exclude_times if not df_exclude_times.empty else None
    except SQLAlchemyError as e:
        logging.error(f"Error loading penalty_exclude_times for game_id {game_id}: {e}")
        return None



def preprocess_and_save_num_players(env_vars, game_id, season):
    """
    Preprocess and save num_players data for a specific game and season.
    """
    engine = get_db_engine()
    game_shifts = load_data(env_vars)["game_shifts"].query(f"game_id == {game_id}")

    if game_shifts.empty:
        logging.warning(f"No shift data for game_id {game_id}. Skipping.")
        return

    # Calculate the number of players on the ice
    shifts_melted = pd.melt(
        game_shifts,
        id_vars=["game_id", "player_id"],
        value_vars=["shift_start", "shift_end"],
    ).sort_values("value", ignore_index=True)
    shifts_melted["change"] = (
        2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
    )
    shifts_melted["num_players"] = shifts_melted["change"].cumsum()
    df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()

    # Remove duplicates before saving
    df_num_players.drop_duplicates(subset=["value"], inplace=True)
    df_num_players["game_id"] = game_id
    df_num_players.rename(columns={"value": "time"}, inplace=True)

    # Save to the database
    table_name = f"num_players_on_ice_{season}"
    try:
        df_num_players.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info(f"Saved num_players data for game_id {game_id} to {table_name}.")
    except Exception as e:
        logging.error(f"Error saving num_players data for game_id {game_id}: {e}")


# Main Function: Get Penalty Exclude Times
# Main Function: Get Penalty Exclude Times
# def get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season):
#     if game_shifts.empty:
#         logging.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
#         return pd.DataFrame()

#     # Cast 'game_id' to int64 using .loc to avoid SettingWithCopyWarning
#     game_shifts.loc[:, "game_id"] = game_shifts["game_id"].astype('int64')
#     game_skater_stats.loc[:, "game_id"] = game_skater_stats["game_id"].astype('int64')
#     game_id = np.int64(game_id)

#     # Load pre-calculated num_players_on_ice data from the database
#     df_num_players = load_num_players_from_db(game_id, season)

#     if df_num_players is None:
#         logging.info(
#             f"No pre-calculated num_players_on_ice found for game_id {game_id}. Calculating manually."
#         )
#         # Merge `team_id` column
#         game_shifts = pd.merge(
#             game_shifts,
#             game_skater_stats[["game_id", "player_id", "team_id"]],
#             on=["game_id", "player_id"],
#             how="left",
#         )

#         # Handle potential duplicate columns from the merge
#         if "team_id_y" in game_shifts.columns:
#             game_shifts = game_shifts.drop(columns=["team_id_y"]).rename(columns={"team_id_x": "team_id"})
#         elif "team_id_x" in game_shifts.columns:
#             game_shifts = game_shifts.rename(columns={"team_id_x": "team_id"})

#         logging.info("Successfully merged team_id into game_shifts DataFrame.")

#         # Calculate the number of players on each team
#         df_num_players_1 = get_num_players(
#             game_shifts[game_shifts["team_id"] == game_shifts.iloc[0]["team_id"]]
#         )
#         df_num_players_2 = get_num_players(
#             game_shifts[game_shifts["team_id"] != game_shifts.iloc[0]["team_id"]]
#         )

#         # Check if 'value' (time) column exists in both DataFrames
#         if "value" not in df_num_players_1.columns or "value" not in df_num_players_2.columns:
#             logging.error("The 'value' column (time) is missing in one of the num_players DataFrames.")
#             return pd.DataFrame()

#         # Rename 'value' column to 'time'
#         df_num_players_1 = df_num_players_1.rename(columns={"value": "time", "num_players": "team_1"})
#         df_num_players_2 = df_num_players_2.rename(columns={"value": "time", "num_players": "team_2"})

#         # Concatenate and sort player counts, then forward fill
#         df_num_players = (
#             pd.concat([df_num_players_1, df_num_players_2])
#             .sort_values("time")
#             .ffill()
#         )

#         # Ensure 'time' column exists after concatenation
#         if "time" not in df_num_players.columns:
#             logging.error("The 'time' column is missing after concatenating num_players DataFrames.")
#             return pd.DataFrame()

#     # Determine penalty exclude times
#     mask = df_num_players["time"].shift(-1) != df_num_players["time"]
#     df_num_players = df_num_players[mask]

#     # Calculate the 'exclude' column based on player counts
#     df_num_players["exclude"] = (
#         (df_num_players["team_1"] != df_num_players["team_2"])
#         | (df_num_players["team_1"] < 5)
#         | (df_num_players["team_2"] < 5)
#         | (df_num_players["team_1"] > 6)  # Account for extra skater if goalie is pulled
#         | (df_num_players["team_2"] > 6)
#     )
#     df_num_players = df_num_players.reset_index(drop=True)

#     logging.info(f"Penalty Exclude Times calculated for game_id {game_id}.")
#     return df_num_players
# def get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season):
#     logging.info(f"Entered get_penalty_exclude_times for game_id {game_id}")

#     if game_shifts.empty:
#         logging.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
#         return pd.DataFrame()

#     try:
#         # Try loading pre-calculated num_players data from the database
#         df_num_players = load_num_players_from_db(game_id, season)

#         # If no pre-calculated data, calculate manually
#         if df_num_players is None or df_num_players.empty:
#             logging.info("No pre-calculated num_players found. Calculating manually.")
#             df_num_players_1 = get_num_players(game_shifts[game_shifts["team_id"] == game_shifts.iloc[0]["team_id"]])
#             df_num_players_2 = get_num_players(game_shifts[game_shifts["team_id"] != game_shifts.iloc[0]["team_id"]])

#             df_num_players_1 = df_num_players_1.rename(columns={"value": "time", "num_players": "team_1"})
#             df_num_players_2 = df_num_players_2.rename(columns={"value": "time", "num_players": "team_2"})

#             df_num_players = pd.concat([df_num_players_1, df_num_players_2]).sort_values("time").ffill()

#         # Ensure 'time' column exists
#         if "time" not in df_num_players.columns:
#             logging.error("The 'time' column is missing in num_players data.")
#             return pd.DataFrame()

#         # Determine penalty exclude times
#         mask = df_num_players["time"].shift(-1) != df_num_players["time"]
#         df_num_players = df_num_players[mask]

#         # Calculate the 'exclude' column based on player counts
#         df_num_players["exclude"] = (
#             (df_num_players["team_1"] != df_num_players["team_2"])
#             | (df_num_players["team_1"] < 5)
#             | (df_num_players["team_2"] < 5)
#             | (df_num_players["team_1"] > 6)
#             | (df_num_players["team_2"] > 6)
#         )
#         df_num_players = df_num_players.reset_index(drop=True)

#         logging.info(f"Penalty Exclude Times calculated for game_id {game_id}.")
#         return df_num_players
#     except Exception as e:
#         logging.error(f"Error calculating penalty exclude times: {e}")
#         return pd.DataFrame()
def get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season):
    logging.info(f"Entered get_penalty_exclude_times for game_id {game_id}")

    # Load pre-calculated num_players data from the database
    df_num_players = load_num_players_from_db(game_id, season)

    if df_num_players is None or "time" not in df_num_players.columns:
        logging.warning("Pre-calculated num_players data is missing or incomplete. Recalculating manually.")

    # Dynamically calculate `team_1` and `team_2` counts
    game_shifts = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left"
    )

    if "team_id_y" in game_shifts.columns:
        game_shifts = game_shifts.drop(columns=["team_id_y"]).rename(columns={"team_id_x": "team_id"})

    # Calculate team-specific counts
    shifts_team_1 = game_shifts[game_shifts["team_id"] == game_shifts.iloc[0]["team_id"]]
    shifts_team_2 = game_shifts[game_shifts["team_id"] != game_shifts.iloc[0]["team_id"]]

    df_team_1 = get_num_players(shifts_team_1).rename(columns={"value": "time", "num_players": "team_1"})
    df_team_2 = get_num_players(shifts_team_2).rename(columns={"value": "time", "num_players": "team_2"})

    # Combine into a single DataFrame
    df_num_players_combined = pd.merge(df_team_1, df_team_2, on="time", how="outer").sort_values("time").ffill()

    # Ensure the necessary columns exist
    if "team_1" not in df_num_players_combined.columns or "team_2" not in df_num_players_combined.columns:
        logging.error("Failed to calculate team-specific player counts.")
        return pd.DataFrame()

    # Calculate the 'exclude' column
    df_num_players_combined["exclude"] = (
        (df_num_players_combined["team_1"] != df_num_players_combined["team_2"]) |
        (df_num_players_combined["team_1"] < 5) |
        (df_num_players_combined["team_2"] < 5) |
        (df_num_players_combined["team_1"] > 6) |  # Extra skater (goalie pulled)
        (df_num_players_combined["team_2"] > 6)
    )

    logging.info(f"Penalty Exclude Times calculated for game_id {game_id}.")
    return df_num_players_combined.reset_index(drop=True)

# def get_penalty_exclude_times_optimized(game_id, season, engine):
def get_penalty_exclude_times_optimized(game_id, season, df_num_players):
    """
    Calculate penalty exclude times based on num_players data.
    """
    logging.info(f"Entered optimized get_penalty_exclude_times for game_id {game_id}")

    try:
        # Ensure df_num_players is not empty
        if df_num_players.empty:
            logging.warning(f"No num_players data found for game_id {game_id}.")
            return pd.DataFrame()

        # Add 'exclude' column where num_players != 12
        df_num_players["exclude"] = df_num_players["num_players"] != 12

        # Remove consecutive duplicate times
        df_num_players = df_num_players[df_num_players["time"].shift(-1) != df_num_players["time"]]

        logging.info(f"Penalty exclude times calculated for game_id {game_id}.")
        return df_num_players[["time", "exclude", "game_id"]]

    except Exception as e:
        logging.error(f"Error calculating exclude times for game_id {game_id}: {e}")
        return pd.DataFrame()



def preprocess_and_save_exclude_times(env_vars, game_id, season):
    """
    Preprocess and save penalty exclude times for a specific game and season.
    """
    engine = get_db_engine()
    logging.info(f"Processing penalty exclude times for game_id {game_id} and season {season}.")

    try:
        # Load num_players data
        df_num_players = load_num_players_from_db(game_id, season)
        if df_num_players is None or df_num_players.empty:
            logging.error(f"Failed to load num_players data for game_id {game_id}. Skipping.")
            return

        # Calculate penalty exclude times
        df_exclude_times = get_penalty_exclude_times_optimized(game_id, season, df_num_players)
        if df_exclude_times.empty:
            logging.error(f"Exclude times calculation failed for game_id {game_id}. Skipping save.")
            return

        # Save exclude times to database
        table_name = f"penalty_exclude_times_{season}"
        df_exclude_times.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info(f"Saved penalty exclude times for game_id {game_id} to table {table_name}.")

    except Exception as e:
        logging.error(f"Error during preprocessing penalty exclude times for game_id {game_id}: {e}")




# Function: Store Number of Players in Database
def store_num_players_to_db(df_num_players, game_id, season):
    table_name = f"num_players_on_ice_{season}"
    try:
        df_num_players["game_id"] = game_id
        df_num_players.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info(
            f"Stored num_players_on_ice data for game_id {game_id} in table {table_name}."
        )
    except SQLAlchemyError as e:
        logging.error(
            f"Error storing num_players_on_ice data for game_id {game_id}: {e}"
        )


def calculate_and_save_corsi_stats(season_game_ids, season):
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    if (
        "game_plays" not in df_master
        or "game_shifts" not in df_master
        or "game_skater_stats" not in df_master
    ):
        logging.error(
            "One or more required dataframes are missing from the loaded data."
        )
        return

    season_corsi_stats = []

    for game_id in season_game_ids:
        # Filter data by the current game_id
        df_game = {name: df[df["game_id"] == game_id] for name, df in df_master.items()}

        # Skip if any game-specific DataFrame is empty
        if (
            df_game["game_shifts"].empty
            or df_game["game_plays"].empty
            or df_game["game_skater_stats"].empty
        ):
            logging.warning(
                f"Skipping game {game_id}: One or more required DataFrames are empty."
            )
            continue

        # Calculate Corsi stats for the current game
        df_corsi = df_game["game_skater_stats"][
            ["game_id", "player_id", "team_id"]
        ].copy()
        corsi_stats = create_corsi_stats(df_corsi, df_game, season)

        if corsi_stats is not None and not corsi_stats.empty:
            season_corsi_stats.append(corsi_stats)
            logging.info(f"Completed Corsi calculation for game {game_id}.")

    # Combine all game data into a single DataFrame
    if season_corsi_stats:
        final_season_df = pd.concat(season_corsi_stats, ignore_index=True)
        output_dir = "/Users/ericwiniecke/Documents/github/cost_cup/corsi_stats"
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist
        output_file = os.path.join(
            output_dir, f"corsi_stats_{season}.csv"
        )  # Set the output path
        final_season_df.to_csv(output_file, index=False)
        logging.info(f"Saved Corsi data for the {season} season to {output_file}.")
    else:
        logging.warning(f"No valid Corsi data was generated for the {season} season.")


# Function: Organize Data by Season
def organize_by_season(seasons, df):
    organized_data = []
    game_df = df.get("game")

    if game_df is None or game_df.empty:
        logging.error("The 'game' DataFrame is missing or empty.")
        return []

    # Print unique seasons and data type for debugging
    logging.info(f"Unique seasons in 'game' DataFrame: {game_df['season'].unique()}")
    logging.info(f"Data type of 'season' column: {game_df['season'].dtype}")

    # Convert the 'season' column to integer if needed
    if game_df["season"].dtype != "int64":
        try:
            game_df["season"] = game_df["season"].astype(int)
            logging.info("Converted 'season' column to integer type.")
        except ValueError as e:
            logging.error(f"Failed to convert 'season' column to integer: {e}")
            return []

    for season in seasons:
        logging.info(f"Processing season {season}")

        # Filter by season
        season_data = game_df[game_df["season"] == season]
        if season_data.empty:
            logging.warning(f"No data found for season {season}.")
            continue

        # Filter related tables using game_id
        game_ids = season_data["game_id"].unique()
        season_df = {
            "game": season_data,
            "game_shifts": df["game_shifts"][
                df["game_shifts"]["game_id"].isin(game_ids)
            ],
            "game_plays": df["game_plays"][df["game_plays"]["game_id"].isin(game_ids)],
            "game_skater_stats": df["game_skater_stats"][
                df["game_skater_stats"]["game_id"].isin(game_ids)
            ],
        }

        organized_data.append((season, season_df))

    return organized_data


def create_corsi_stats(df_corsi, df, season):
    logging.info("Entered create_corsi_stats")

    # Initialize Corsi statistics columns with zeros for all players
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

    # Ensure the 'time' column is created in game_plays
    if "game_plays" in df:
        game_plays = df["game_plays"]

        if "time" not in game_plays.columns:
            # Calculate 'time' based on period and periodTime
            game_plays["time"] = game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
            logging.info("Calculated 'time' column in game_plays.")

        # Verify the 'time' column exists
        if "time" not in game_plays.columns:
            logging.error("The 'time' column is missing from game_plays.")
            return

        # Filter game_plays to include only relevant events
        game_plays = game_plays[game_plays["event"].isin(relevant_events)]
        game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])

        # Log the first few rows to verify the 'time' column
        logging.info("First few rows of game_plays after calculating 'time':")
        logging.info(game_plays[["period", "periodTime", "time", "event"]].head())
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
                logging.error("'time' column missing in plays_game after filtering by game_id.")
                return
            else:
                logging.info(f"'time' column verified in plays_game for game_id {game_id}.")

            # Merge team_id into game_shifts
            game_shifts = pd.merge(
                game_shifts,
                df["game_skater_stats"][["game_id", "player_id", "team_id"]],
                on=["game_id", "player_id"],
                how="left",
            )

            game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
            game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

            # Calculate penalty exclude times
            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            df_num_players = get_penalty_exclude_times(game_shifts, gss, game_id, season).reset_index(drop=True)

            # Exclude non-even strength plays using the calculated exclude times
            idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
            idx[idx < 0] = 0
            mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
            plays_game = plays_game.loc[~mask]

    # Loop through each play in the filtered game_plays
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

        # Corsi calculations and log updates
        if event["event"] in ["Shot", "Goal", "Missed Shot"]:
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"
            ] += 1
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_against"
            ] += 1
            logging.info(f"{event['event']} - Updated CF and CA for the event.")

        elif event["event"] == "Blocked Shot":
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_against"
            ] += 1
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]), "corsi_for"
            ] += 1
            logging.info("Blocked Shot - Updated CF and CA for blocked shot event.")

    # Calculate the final Corsi statistics
    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]

    # Calculate CF% (Corsi For Percentage)
    df_corsi["CF_Percent"] = (
        (
            df_corsi["corsi_for"] /
            (df_corsi["corsi_for"] + df_corsi["corsi_against"])
        ).fillna(0) * 100
    ).round(4)

    # Log summary of the final Corsi statistics
    logging.info("Corsi calculations complete. Summary of first few rows:")
    logging.info(df_corsi.head(5))

    return df_corsi

