import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from db_utils import get_db_engine
from load_data import load_data, get_env_vars
from corsi_utils import get_num_players, get_penalty_exclude_times
import pandas as pd

# Logger configuration
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("penalty_exclude_times.log"),  # Save to a file
        logging.StreamHandler()  # Print to console
    ]
)

def preprocess_excluded_times_for_season(season, game_ids):
    """
    Calculate excluded times for every game in a season and save to a PostgreSQL table.

    Args:
        season (int): The season year (e.g., 20152016).
        game_ids (list): List of game IDs for the season.
    """
    engine = get_db_engine()
    logging.info(f"Starting penalty exclude times calculation for season {season}.")

    table_name = f"penalty_exclude_times_{season}"

    # Step 1: Ensure the table exists
    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    time FLOAT NOT NULL,
                    exclude BOOLEAN NOT NULL,
                    game_id BIGINT NOT NULL,
                    PRIMARY KEY (game_id, time)
                );
            """))
        logging.info(f"Table {table_name} ensured in database.")
    except SQLAlchemyError as e:
        logging.error(f"Database error during table creation: {e}")
        return
    except Exception as e:
        logging.error(f"Unexpected error during table creation: {e}")
        return

    # Step 2: Process each game
    for game_id in game_ids:
        try:
            logging.info(f"Processing game_id {game_id}.")
            df_master = load_data(get_env_vars())  # Load all data
            game_shifts = df_master["game_shifts"].query(f"game_id == {game_id}")
            game_skater_stats = df_master["game_skater_stats"].query(f"game_id == {game_id}")

            # Skip if necessary data is missing
            if game_shifts.empty or game_skater_stats.empty:
                logging.warning(f"Skipping game_id {game_id} due to missing data.")
                continue

            # Calculate number of players on the ice
            df_num_players = get_num_players(game_shifts)
            df_num_players["game_id"] = game_id  # Add game_id for context

            # Use get_penalty_exclude_times to calculate exclusions
            df_exclude_times = get_penalty_exclude_times(game_shifts, game_skater_stats, game_id, season)

            if df_exclude_times.empty:
                logging.warning(f"No exclude times calculated for game_id {game_id}. Skipping.")
                continue

            # Ensure game_id is correctly set
            df_exclude_times["game_id"] = game_id

            # Log the first few rows to verify correctness
            logging.info(f"First few rows of df_exclude_times for game_id {game_id}:\n{df_exclude_times.head()}")

            # Save to PostgreSQL
            try:
                df_exclude_times.to_sql(table_name, con=engine, if_exists="append", index=False)
                logging.info(f"Saved exclude times for game_id {game_id} to {table_name}.")
            except SQLAlchemyError as e:
                logging.error(f"Database error for game_id {game_id}: {e}")
            except Exception as e:
                logging.error(f"Error processing game_id {game_id}: {e}")

        except Exception as e:
            logging.error(f"Unexpected error during processing of game_id {game_id}: {e}")

    logging.info(f"Completed penalty exclude times calculation for season {season}.")

if __name__ == "__main__":
    # Example usage
    season = 20152016
    # Replace with a function that fetches all game IDs for the season
    game_ids = [2015020002, 2015020003, 2015020004]  # Example game IDs
    preprocess_excluded_times_for_season(season, game_ids)


