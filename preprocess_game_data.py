"""
Eric Winiecke

November 9, 2024
Script to preprocess game data:
- Calculate and save num_players_on_ice
- Calculate and save penalty_exclude_times
"""

import logging
import os

import pandas as pd
#from sqlalchemy import create_engine


from corsi_utils import get_num_players, get_penalty_exclude_times, load_num_players_from_db #organize_by_season
from db_utils import get_db_engine
from load_data import load_data

# Set up logging
log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing_II_V2.log"
log_directory = os.path.dirname(log_file_path)
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path, mode="w"), logging.StreamHandler()],
)

# Database connection
engine = get_db_engine()


def preprocess_and_save_exclude_times(env_vars, seasons):
    """
    Preprocess and save penalty exclude times for each game in the specified seasons,
    using pre-calculated num_players data when available.
    """
    engine = get_db_engine()
    df_master = load_data(env_vars)

    for season in seasons:
        logging.info(f"Processing penalty exclude times for season {season}.")
        penalty_exclude_times_data = []

        # Get all unique game IDs for the season
        game_ids = df_master["game"].query(f"season == {season}")["game_id"].unique()

        for game_id in game_ids:
            logging.info(f"Processing game_id {game_id}.")
            game_shifts = df_master["game_shifts"].query(f"game_id == {game_id}")
            game_skater_stats = df_master["game_skater_stats"].query(f"game_id == {game_id}")

            if game_shifts.empty or game_skater_stats.empty:
                logging.warning(f"Missing data for game_id {game_id}. Skipping.")
                continue

            # Use pre-calculated num_players data if available
            df_num_players = load_num_players_from_db(game_id, season)
            if df_num_players is None:
                logging.info(f"No pre-calculated num_players found for game_id {game_id}. Calculating manually.")
                df_num_players = preprocess_and_save_num_players(env_vars, [season])

            try:
                exclude_times_df = get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season)
                if exclude_times_df.empty:
                    logging.warning(f"No exclude times calculated for game_id {game_id}. Skipping.")
                    continue

                exclude_times_df["game_id"] = game_id
                penalty_exclude_times_data.append(exclude_times_df)

            except Exception as e:
                logging.error(f"Error calculating exclude times for game_id {game_id}: {e}")
                continue

        # Concatenate and save the results to the database
        if penalty_exclude_times_data:
            df_exclude_all = pd.concat(penalty_exclude_times_data, ignore_index=True)
            table_name = f"penalty_exclude_times_{season}"
            try:
                df_exclude_all.to_sql(table_name, con=engine, if_exists="replace", index=False)
                logging.info(f"Saved penalty exclude times to table {table_name}.")
            except Exception as e:
                logging.error(f"Error saving to table {table_name}: {e}")
        else:
            logging.warning(f"No penalty exclude times found for season {season}.")

    logging.info("Preprocessing and saving of penalty exclude times completed.")



def preprocess_and_save_num_players(env_vars, seasons):
    """
    Preprocess and save the number of players on ice for each game in the specified seasons.
    """
    engine = get_db_engine()
    df_master = load_data(env_vars)

    for season in seasons:
        logging.info(f"Processing num_players for season {season}.")
        num_players_data = []

        # Get all unique game IDs for the season
        game_ids = df_master["game"].query(f"season == {season}")["game_id"].unique()

        for game_id in game_ids:
            logging.info(f"Processing game_id {game_id}.")
            game_shifts = df_master["game_shifts"].query(f"game_id == {game_id}")

            if game_shifts.empty:
                logging.warning(f"No shift data for game_id {game_id}. Skipping.")
                continue

            # Check if num_players data already exists in the database
            existing_data = load_num_players_from_db(game_id, season)
            if existing_data is not None:
                logging.info(f"Using pre-existing num_players data for game_id {game_id}.")
                continue

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

            # Rename 'value' column to 'time' and filter changes in player count
            df_num_players = df_num_players.rename(columns={"value": "time"})
            df_num_players = df_num_players[df_num_players["num_players"].shift() != df_num_players["num_players"]]
            df_num_players["game_id"] = game_id
            num_players_data.append(df_num_players)

        if num_players_data:
            df_num_players_all = pd.concat(num_players_data, ignore_index=True)
            table_name = f"num_players_{season}"
            try:
                df_num_players_all.to_sql(table_name, con=engine, if_exists="replace", index=False)
                logging.info(f"Saved num_players data to table {table_name}.")
            except Exception as e:
                logging.error(f"Error saving num_players data to table {table_name}: {e}")
        else:
            logging.warning(f"No num_players data found for season {season}.")

    logging.info("Preprocessing and saving of num_players completed.")


def main():
    import logging
    from load_data import get_env_vars, get_db_engine, load_data
    from corsi_utils import get_num_players, get_penalty_exclude_times
    from preprocess_game_data import preprocess_and_save_num_players, preprocess_and_save_exclude_times

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Step 1: Get environment variables and initialize the database engine
    env_vars = get_env_vars()
    engine = get_db_engine(env_vars)

    # Step 2: Define a subset of game IDs for testing
    test_game_ids = [2015020002, 2015020003, 2015020004, 2015020005, 2015020006]
    seasons = [20152016]  # Update this if you want to test other seasons

    # Step 3: Load data for testing
    try:
        df_master = load_data(env_vars)
        logging.info("Data loaded successfully for testing.")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        return

    # Step 4: Preprocess and save num_players for the test games
    logging.info("Starting preprocessing of num_players for test games.")
    try:
        preprocess_and_save_num_players(env_vars, seasons)
        logging.info("Preprocessing and saving of num_players completed successfully.")
    except Exception as e:
        logging.error(f"Error during preprocessing num_players: {e}")

    # Step 5: Preprocess and save penalty exclude times for the test games
    logging.info("Starting preprocessing of penalty exclude times for test games.")
    try:
        preprocess_and_save_exclude_times(env_vars, seasons)
        logging.info("Preprocessing and saving of penalty exclude times completed successfully.")
    except Exception as e:
        logging.error(f"Error during preprocessing penalty exclude times: {e}")

    # Step 6: Verify the results by loading some test data
    for game_id in test_game_ids:
        try:
            # Load num_players data from the database
            table_name_num_players = f"num_players_on_ice_{seasons[0]}"
            num_players_df = pd.read_sql(
                f"SELECT * FROM {table_name_num_players} WHERE game_id = {game_id}",
                con=engine
            )
            logging.info(f"Loaded num_players data for game_id {game_id}:")
            logging.info(num_players_df.head())

            # Load penalty exclude times data from the database
            table_name_exclude_times = f"penalty_exclude_times_{seasons[0]}"
            exclude_times_df = pd.read_sql(
                f"SELECT * FROM {table_name_exclude_times} WHERE game_id = {game_id}",
                con=engine
            )
            logging.info(f"Loaded penalty exclude times data for game_id {game_id}:")
            logging.info(exclude_times_df.head())

        except Exception as e:
            logging.error(f"Error loading test data for game_id {game_id}: {e}")

    logging.info("All preprocessing steps completed successfully.")

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()






