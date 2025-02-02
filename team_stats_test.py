
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas as pd
import numpy as np
from load_data import get_env_vars, load_data, get_db_engine
# from corsi_utils import get_num_players, get_penalty_exclude_times
from corsi_utils import get_penalty_exclude_times_optimized as get_penalty_exclude_times
from corsi_utils import load_num_players_from_db,  load_exclude_times_from_db
from corsi_utils import (
    get_penalty_exclude_times_optimized,
    preprocess_and_save_exclude_times,
    preprocess_and_save_num_players,
)






# Define the log file path
log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing_test.log"
log_directory = os.path.dirname(log_file_path)

# Ensure the log directory exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
# Configure RotatingFileHandler (Max size 5 MB, keep up to 3 backup files)
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

# def calculate_shot_totals_optimized(game_id, season):
#     try:
#         # Get environment variables
#         env_vars = get_env_vars()
#         df_master = load_data(env_vars)

#         # Filter data by game_id early to reduce size
#         game_plays = df_master["game_plays"].query(f"game_id == {game_id}").copy()
#         game_shifts = df_master["game_shifts"].query(f"game_id == {game_id}").copy()
#         game_skater_stats = df_master["game_skater_stats"].query(f"game_id == {game_id}").copy()

#         # Check if data is loaded correctly
#         if game_plays.empty or game_shifts.empty or game_skater_stats.empty:
#             logging.error(f"No data found for game_id {game_id}.")
#             return

#         # Convert periodTime to 'time' early and explicitly
#         if "time" not in game_plays.columns:
#             game_plays.loc[:, "time"] = (
#                 game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
#             )
#             logging.info("Converted 'periodTime' to 'time' in game_plays.")

#         # Log the first few rows of game_plays to confirm 'time' column exists
#         logging.info(f"game_plays sample data:\n{game_plays[['game_id', 'period', 'periodTime', 'time']].head()}")

#         # Cache num_players to avoid recalculating
#         df_num_players = get_num_players(game_shifts)
#         df_num_players["game_id"] = game_id

#         # Establish database connection using env_vars
#         engine = get_db_engine(env_vars)
#         table_name = f"num_players_on_ice_{season}"
#         df_num_players.to_sql(table_name, con=engine, if_exists="append", index=False)
#         logging.info(f"Saved num_players data for game_id {game_id} to {table_name}.")

#         # Get exclude times using cached num_players data
#         exclude_times_df = get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season)
#         if exclude_times_df.empty:
#             logging.warning(f"No exclude times calculated for game_id {game_id}.")
#             return

#         # Filter even-strength plays
#         idx = exclude_times_df["time"].searchsorted(game_plays["time"]) - 1
#         idx[idx < 0] = 0
#         mask = exclude_times_df["exclude"][idx].reset_index(drop=True).to_numpy()
#         even_strength_plays = game_plays.loc[~mask]

#         # Aggregate shot totals for and against each team
#         shot_totals_for = (
#             even_strength_plays.groupby("team_id_for")
#             .agg(
#                 total_goals=("event", lambda x: (x == "Goal").sum()),
#                 total_shots=("event", lambda x: (x == "Shot").sum()),
#                 total_missed_shots=("event", lambda x: (x == "Missed Shot").sum()),
#                 total_blocked_shots_against=("event", lambda x: (x == "Blocked Shot").sum())
#             )
#             .reset_index()
#             .rename(columns={"team_id_for": "team_id"})
#         )

#         shot_totals_against = (
#             even_strength_plays.groupby("team_id_against")
#             .agg(
#                 total_goals_against=("event", lambda x: (x == "Goal").sum()),
#                 total_shots_against=("event", lambda x: (x == "Shot").sum()),
#                 total_missed_shots_against=("event", lambda x: (x == "Missed Shot").sum()),
#                 total_blocked_shots_for=("event", lambda x: (x == "Blocked Shot").sum())
#             )
#             .reset_index()
#             .rename(columns={"team_id_against": "team_id"})
#         )

#         # Merge the results
#         combined_shot_totals = pd.merge(
#             shot_totals_for, shot_totals_against, on="team_id", how="outer"
#         ).fillna(0)

#         logging.info(f"Shot totals calculated for game_id {game_id}.")
#         logging.info("\n" + combined_shot_totals.to_string(index=False))

#         # Save the results to a CSV file
#         combined_shot_totals.to_csv(f"shot_totals_game_{game_id}.csv", index=False)
#         logging.info(f"Results saved to shot_totals_game_{game_id}.csv")

#         return combined_shot_totals

#     except Exception as e:
#         logging.error(f"Error during shot totals calculation: {e}")
#         return




# def preprocess_and_save_exclude_times(env_vars, game_id, season):
#     engine = get_db_engine(env_vars)
#     df_master = load_data(env_vars)

#     logging.info(f"Processing penalty exclude times for game_id {game_id} and season {season}.")
#     df_num_players = load_num_players_from_db(game_id, season)
#     if df_num_players is not None:
#         df_exclude_times = get_penalty_exclude_times_optimized(game_id, season, df_num_players)
#     else:
#         logging.error(f"No num_players data available for game_id {game_id}.")
#         return

#     try:
#         # Load game data
#         game_shifts = df_master["game_shifts"].query(f"game_id == {game_id}")
#         game_skater_stats = df_master["game_skater_stats"].query(f"game_id == {game_id}")

#         if game_shifts.empty or game_skater_stats.empty:
#             logging.warning(f"Missing data for game_id {game_id}. Skipping.")
#             return

#         # Calculate penalty exclude times
#         df_exclude_times = get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season)

#         if df_exclude_times.empty:
#             logging.error(f"Exclude times calculation failed for game_id {game_id}. Skipping save.")
#             return

#         # Save to database with conflict handling
#         table_name = f"penalty_exclude_times_{season}"
#         try:
#             with engine.connect() as conn:
#                 existing_times = pd.read_sql(
#                     f"SELECT time FROM {table_name} WHERE game_id = {game_id}", conn
#                 )["time"].tolist()
#                 df_exclude_times = df_exclude_times[~df_exclude_times["time"].isin(existing_times)]

#             if not df_exclude_times.empty:
#                 df_exclude_times.to_sql(table_name, con=engine, if_exists="append", index=False)
#                 logging.info(f"Saved penalty exclude times for game_id {game_id} to table {table_name}.")
#             else:
#                 logging.info(f"No new penalty exclude times to insert for game_id {game_id}.")
#         except Exception as e:
#             logging.error(f"Error saving penalty exclude times for game_id {game_id}: {e}")

#     except Exception as e:
#         logging.error(f"Error during preprocessing penalty exclude times for game_id {game_id}: {e}")


def calculate_team_event_totals(game_id, season):
    """
    Calculate the total number of goals, shots, missed shots, and blocked shots
    for and against each team, excluding periods when teams are not at even strength.
    """
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    if not all(key in df_master for key in ["game_plays", "game_shifts", "game_skater_stats"]):
        logging.error("Required dataframes are missing from the loaded data.")
        return

    # Ensure 'time' column is created in game_plays
    if "time" not in df_master["game_plays"].columns:
        df_master["game_plays"] = df_master["game_plays"].copy()
        df_master["game_plays"]["time"] = (
            df_master["game_plays"]["periodTime"] + (df_master["game_plays"]["period"] - 1) * 1200
        )
        logging.info("Calculated 'time' column in game_plays.")

    # Rename 'time' column to 'play_time' to avoid conflicts
    df_master["game_plays"].rename(columns={"time": "play_time"}, inplace=True)

    # Filter data by game_id
    df_master = {name: df[df["game_id"] == game_id] for name, df in df_master.items()}
    logging.info(f"Data filtered for game_id {game_id}.")

    game_plays = df_master["game_plays"]
    game_shifts = df_master["game_shifts"]
    game_skater_stats = df_master["game_skater_stats"]

    # Load penalty exclude times from database
    df_exclude_times = load_exclude_times_from_db(game_id, season)
    if df_exclude_times is None or df_exclude_times.empty:
        logging.error(f"Exclude times data missing for game_id {game_id}.")
        return

    # Filter even-strength plays
    idx = df_exclude_times["time"].searchsorted(game_plays["play_time"]) - 1
    idx[idx < 0] = 0
    mask = df_exclude_times["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
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

    event_totals_combined = pd.merge(
        event_totals_for, event_totals_against, on="team_id", how="outer"
    ).fillna(0)

    logging.info("Final aggregated team event totals:")
    logging.info("\n" + event_totals_combined.to_string(index=False))
    return event_totals_combined




def main():
    from load_data import get_env_vars

    env_vars = get_env_vars()
    season = 20152016
    game_id = 2015020002  # Single game for testing

    # Preprocess and save num_players data
    preprocess_and_save_num_players(env_vars, game_id, season)

    # Preprocess and save exclude times
    preprocess_and_save_exclude_times(env_vars, game_id, season)

    # Calculate team event totals
    try:
        event_totals = calculate_team_event_totals(game_id, season)
        if event_totals is not None:
            print(event_totals)
        else:
            logging.error("Failed to calculate team event totals.")
    except Exception as e:
        logging.error(f"Error during team event totals calculation: {e}")

if __name__ == "__main__":
    main()








# if __name__ == "__main__":
#     # Run the function for a single game (2015020002) to avoid long processing time
#     game_id = 2015020002
#     season = 20152016

#     result = calculate_shot_totals_optimized(game_id, season)
#     if result is not None:
#         print("Shot totals calculated successfully:")
#         print(result)
#     else:
#         print("Failed to calculate shot totals.")



# if __name__ == "__main__":
#     env_vars = get_env_vars()
#     preprocess_and_save_num_players(env_vars, season=20152016)

# if __name__ == "__main__":
#     env_vars = get_env_vars()
#     preprocess_and_save_exclude_times(env_vars, game_id=2015020002, season=20152016)


# if __name__ == "__main__":
#     game_id = 2015020002  # Test with a single game ID
#     try:
#         result = calculate_team_event_totals(game_id=game_id)
#         if result is not None:
#             print("Shot totals calculated successfully:")
#             print(result)
#             result.to_csv(f"team_event_totals_{game_id}.csv", index=False)
#             print(f"Results saved to team_event_totals_{game_id}.csv")
#         else:
#             print("Failed to calculate shot totals.")
#     except Exception as e:
#         logging.error(f"Unexpected error during testing: {e}")


