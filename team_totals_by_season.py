"""
Eric Winiecke
November 9, 2024

Program to gather cumulative Corsi stats for each team by season.
"""

import logging

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from corsi_utils import (
    load_exclude_times_from_db,
    load_num_players_from_db,
    organize_by_season,
)
from db_utils import get_db_engine
from load_data import load_data, get_env_vars

env_vars = get_env_vars()  # Get the environment variables
df_master = load_data(env_vars)


# Database connection
engine = get_db_engine()


def calculate_team_season_corsi_totals(season_game_ids, season):
    """
    Calculate cumulative Corsi event totals for each team_id across an entire season.
    """
    cumulative_totals = []

    for game_id in season_game_ids:
        try:
            # Cast game_id to int64 to avoid type mismatch
            game_id = int(game_id)

            # Load preprocessed penalty exclude times and num players from the database
            exclude_times = load_exclude_times_from_db(game_id, season)
            num_players_df = load_num_players_from_db(game_id, season)

            # Load game plays data
            game_plays_query = f"SELECT * FROM game_plays WHERE game_id = {game_id}"
            game_plays = pd.read_sql(game_plays_query, engine)

            # Check for missing game plays data
            if game_plays.empty:
                logging.warning(f"Skipping game_id {game_id}: 'game_plays' is empty.")
                continue

            # Create 'time' column if it doesn't exist
            if "time" not in game_plays.columns:
                game_plays["time"] = (
                    game_plays["periodTime"] + (game_plays["period"] - 1) * 1200
                )

            # Filter for even-strength plays using exclude times
            idx = exclude_times["time"].searchsorted(game_plays["time"]) - 1
            idx[idx < 0] = 0
            mask = exclude_times["exclude"].iloc[idx].reset_index(drop=True).to_numpy()
            even_strength_plays = game_plays.loc[~mask]

            if even_strength_plays.empty:
                logging.warning(
                    f"No even-strength plays for game_id {game_id}. Skipping."
                )
                continue

            # Aggregate Corsi events for 'team_id_for' and 'team_id_against'
            event_totals_for = (
                even_strength_plays.groupby("team_id_for")
                .agg(
                    CF=(
                        "event",
                        lambda x: (x.isin(["Shot", "Goal", "Missed Shot"])).sum(),
                    ),
                    CA=("event", lambda x: (x == "Blocked Shot").sum()),
                )
                .reset_index()
                .rename(columns={"team_id_for": "team_id"})
            )

            event_totals_against = (
                even_strength_plays.groupby("team_id_against")
                .agg(
                    CF=("event", lambda x: (x == "Blocked Shot").sum()),
                    CA=(
                        "event",
                        lambda x: (x.isin(["Shot", "Goal", "Missed Shot"])).sum(),
                    ),
                )
                .reset_index()
                .rename(columns={"team_id_against": "team_id"})
            )

            # Merge the results using 'team_id'
            event_totals = pd.merge(
                event_totals_for,
                event_totals_against,
                on="team_id",
                how="outer",
                suffixes=("_for", "_against"),
            ).fillna(0)

            # Calculate C (Corsi) and CF%
            event_totals["C"] = event_totals["CF"] - event_totals["CA"]
            event_totals["CF%"] = (
                event_totals["CF"] / (event_totals["CF"] + event_totals["CA"]) * 100
            ).round(2)

            # Append results for the current game
            cumulative_totals.append(event_totals)

        except Exception as e:
            logging.error(f"Error processing game_id {game_id}: {e}")
            continue

    # Combine all game totals into a cumulative DataFrame for the season
    if cumulative_totals:
        season_totals_df = pd.concat(cumulative_totals, ignore_index=True)
        season_team_totals = season_totals_df.groupby("team_id").sum().reset_index()

        # Save cumulative totals to PostgreSQL
        table_name = f"team_agg_corsi_{season}"
        season_team_totals.to_sql(
            table_name, con=engine, if_exists="replace", index=False
        )
        logging.info(f"Cumulative Corsi totals saved to table {table_name}.")
        return season_team_totals
    else:
        logging.warning(f"No valid Corsi data aggregated for season {season}.")
        return None


if __name__ == "__main__":
    engine = get_db_engine()  # Get the engine directly
    # seasons = ["20152016", "20162017", "20172018"]
    seasons = [20152016, 20162017, 20172018]

    df_master = load_data(env_vars)  # No need to pass env_vars

    organized_data = organize_by_season(seasons, df_master)
    for season, df in organized_data:
        season_game_ids = df["game"]["game_id"].unique()
        team_corsi_totals = calculate_team_season_corsi_totals(season_game_ids, season)

        if team_corsi_totals is not None:
            logging.info(
                f"Aggregated Corsi stats successfully calculated for season {season}."
            )
        else:
            logging.warning(f"No data found for season {season}.")



