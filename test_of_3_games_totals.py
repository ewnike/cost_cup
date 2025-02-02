import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from db_utils import get_db_engine
from load_data import load_data, get_env_vars

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def calculate_team_totals_for_games(season, game_ids):
    """
    Calculate team totals for a list of game_ids using penalty exclude times.

    Args:
        season (int): The season year (e.g., 20152016).
        game_ids (list): List of game IDs to process.

    Returns:
        pd.DataFrame: Aggregated team totals for all games.
    """
    # engine = create_engine("postgresql+psycopg2://user:password@host:port/dbname")  # Replace with your connection string
    engine = get_db_engine()
    results = []

    for game_id in game_ids:
        try:
            logging.info(f"Processing totals for game_id {game_id}.")

            # Load penalty exclude times
            query_penalty = text(f"""
                SELECT time, exclude
                FROM penalty_exclude_times_{season}
                WHERE game_id = :game_id
            """)
            penalty_exclude_times = pd.read_sql(query_penalty, engine, params={"game_id": game_id})

            if penalty_exclude_times.empty:
                logging.warning(f"No penalty exclude times found for game_id {game_id}. Skipping.")
                continue

            # Load game plays
            # query_game_plays = text(f"""
            #     SELECT game_id, team_id_for, team_id_against, event
            #     FROM game_plays
            #     WHERE game_id = :game_id
            # """)

            query_game_plays = text("""
                SELECT 
                    game_id, 
                    team_id_for, 
                    team_id_against, 
                    event, 
                    ((period - 1) * 1200 + "periodTime") AS time
                FROM game_plays
                WHERE game_id = :game_id
            """)



            game_plays = pd.read_sql(query_game_plays, engine, params={"game_id": game_id})

            if game_plays.empty:
                logging.warning(f"No game plays found for game_id {game_id}. Skipping.")
                continue

            # Exclude non-even strength plays
            idx = penalty_exclude_times["time"].searchsorted(game_plays["time"]) - 1
            idx[idx < 0] = 0
            mask = penalty_exclude_times["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
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

            # Merge results
            game_totals = pd.merge(
                event_totals_for, event_totals_against, on="team_id", how="outer"
            ).fillna(0)

            # Add game_id for context
            game_totals["game_id"] = game_id

            results.append(game_totals)

        except SQLAlchemyError as e:
            logging.error(f"Database error for game_id {game_id}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error processing game_id {game_id}: {e}")

    if results:
        # Combine all game totals
        final_results = pd.concat(results, ignore_index=True)
        logging.info("Team totals calculation completed for all games.")
        return final_results
    else:
        logging.warning("No team totals were calculated.")
        return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    season = 20152016
    game_ids = [2015020002, 2015020003, 2015020004]  # Example game IDs
    team_totals = calculate_team_totals_for_games(season, game_ids)
    if not team_totals.empty:
        print(team_totals)
    else:
        logging.warning("No results to display.")
