# team_stats.py

import logging
import pandas as pd
from corsi_utils import load_exclude_times_from_db, load_num_players_from_db
from db_utils import get_db_engine
from load_data import fetch_game_ids

#from load_data import fetch_game_ids
# from team_stats import calculate_team_season_totals_for_debug

def calculate_team_season_totals_for_debug(season_game_ids, season):
    """
    Calculate basic team stats for each team_id across an entire season for debugging.
    """
    cumulative_totals = []

    # Get the database engine
    engine = get_db_engine()

    for game_id in season_game_ids:
        try:
            # Convert game_id to int64 to avoid type mismatch issues
            game_id = int(game_id)

            # Load pre-calculated exclude times and num players from the database
            exclude_times = load_exclude_times_from_db(game_id, season)
            num_players_df = load_num_players_from_db(game_id, season)

            # Load game plays data from the database
            game_plays_query = f"SELECT * FROM game_plays WHERE game_id = {game_id}"
            game_plays = pd.read_sql(game_plays_query, engine)

            # Check if 'game_plays' is empty
            if game_plays.empty:
                logging.warning(f"Skipping game_id {game_id}: 'game_plays' is empty.")
                continue

            # Create 'time' column if it doesn't already exist
            if "time" not in game_plays.columns:
                game_plays["time"] = (
                    game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
                )

            # Filter for even-strength plays using exclude times
            idx = exclude_times["time"].searchsorted(game_plays["time"]) - 1
            idx[idx < 0] = 0
            mask = exclude_times["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
            even_strength_plays = game_plays.loc[~mask]

            # Skip games without any even-strength plays
            if even_strength_plays.empty:
                logging.warning(f"No even-strength plays for game_id {game_id}. Skipping.")
                continue

            # Aggregate basic play counts for 'team_id_for' and 'team_id_against'
            team_stats_for = (
                even_strength_plays.groupby("team_id_for")
                .size()
                .reset_index(name="total_plays_for")
                .rename(columns={"team_id_for": "team_id"})
            )

            team_stats_against = (
                even_strength_plays.groupby("team_id_against")
                .size()
                .reset_index(name="total_plays_against")
                .rename(columns={"team_id_against": "team_id"})
            )

            # Merge results using 'team_id'
            team_totals = pd.merge(
                team_stats_for,
                team_stats_against,
                on="team_id",
                how="outer",
                suffixes=("_for", "_against"),
            ).fillna(0)

            # Append aggregated totals for this game
            cumulative_totals.append(team_totals)

        except Exception as e:
            logging.error(f"Error processing game_id {game_id}: {e}")
            continue

    # Combine all game totals into a cumulative DataFrame for the season
    if cumulative_totals:
        season_totals_df = pd.concat(cumulative_totals, ignore_index=True)
        season_team_totals = season_totals_df.groupby("team_id").sum().reset_index()

        # Save cumulative totals to PostgreSQL
        table_name = f"team_agg_totals_{season}"
        season_team_totals.to_sql(
            table_name, con=engine, if_exists="replace", index=False
        )
        logging.info(f"Cumulative team totals saved to table {table_name}.")
        return season_team_totals
    else:
        logging.warning(f"No valid data aggregated for season {season}.")
        return None


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Initialize the database engine
    engine = get_db_engine()

    # Define the season for debugging (e.g., 20172018)
    season = 20172018

    # Fetch game IDs for the specified season
    try:
        season_game_ids = fetch_game_ids(engine, str(season))
    except Exception as e:
        logging.error(f"Failed to fetch game IDs for season {season}: {e}")
        exit(1)

    # Check if any game IDs were found
    if not season_game_ids:
        logging.error(f"No game IDs found for season {season}. Exiting.")
        exit(1)

    logging.info(f"Found {len(season_game_ids)} game IDs for season {season}. Starting aggregation...")

    # Run the aggregation function
    season_totals = calculate_team_season_totals_for_debug(season_game_ids, season)

    # Check the result and log the outcome
    if season_totals is not None:
        logging.info(f"Season totals for {season} successfully calculated and saved.")
    else:
        logging.warning(f"No data was aggregated for season {season}.")
