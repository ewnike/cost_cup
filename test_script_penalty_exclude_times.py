import logging
from logging.handlers import RotatingFileHandler
import os
import pandas as pd

# Setup Logging
log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing_II.log"
log_directory = os.path.dirname(log_file_path)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    print(f"Created log directory: {log_directory}")
else:
    print(f"Log directory exists: {log_directory}")

rotating_handler = RotatingFileHandler(log_file_path, maxBytes=5 * 1024 * 1024, backupCount=3)
rotating_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
rotating_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(rotating_handler)
logger.addHandler(logging.StreamHandler())

logger.info("Logger configured successfully with RotatingFileHandler.")
print(f"Logging to file: {log_file_path}")

# Helper Functions
def verify_penalty(game_id, time, game_plays, game_shifts):
    required_columns = ["team_id_for", "team_id_against", "period", "periodTime", "event"]
    for col in required_columns:
        if col not in game_plays.columns:
            logging.error(f"Missing column '{col}' in game_plays.")
            return "None"

    plays = game_plays[game_plays["game_id"] == game_id]
    plays["event_time"] = (plays["period"] - 1) * 1200 + plays["periodTime"]
    penalties = plays[(plays["event"] == "Penalty") & (plays["event_time"] == time)]

    if penalties.empty:
        return "None"

    unique_teams = penalties["team_id_for"].nunique()
    return "Offsetting" if unique_teams > 1 else "Penalty"

def get_num_players(shift_df):
    shifts_melted = pd.melt(shift_df, id_vars=["game_id", "player_id"], value_vars=["shift_start", "shift_end"]).sort_values("value", ignore_index=True)
    shifts_melted["change"] = 2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
    shifts_melted["num_players"] = shifts_melted["change"].cumsum()
    df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()
    return df_num_players[df_num_players["num_players"].shift() != df_num_players["num_players"]].reset_index(drop=True)

def get_penalty_exclude_times(game_shifts, game_skater_stats, game_plays):
    if game_shifts.empty:
        logging.warning("game_shifts is empty.")
        return pd.DataFrame()

    game_shifts = pd.merge(game_shifts, game_skater_stats[["game_id", "player_id", "team_id"]],
                           on=["game_id", "player_id"], how="left")
    game_shifts = game_shifts.drop(columns=["team_id_y"], errors="ignore").rename(columns={"team_id_x": "team_id"})

    # Divide shifts by team
    team_1 = game_shifts.iloc[0]["team_id"]
    shifts_1 = game_shifts[game_shifts["team_id"] == team_1]
    shifts_2 = game_shifts[game_shifts["team_id"] != team_1]

    df_num_players_1 = get_num_players(shifts_1).rename(columns={"value": "time", "num_players": "team_1"})
    df_num_players_2 = get_num_players(shifts_2).rename(columns={"value": "time", "num_players": "team_2"})
    df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values("time", ignore_index=True).ffill()

    # Ensure `game_id` is preserved
    df_exclude["game_id"] = game_shifts["game_id"].iloc[0]

    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask]

    exclude_list = []
    for _, row in df_exclude.iterrows():
        # Safeguard against missing `game_id`
        if "game_id" not in row or pd.isna(row["game_id"]):
            logging.error("Missing game_id in df_exclude row.")
            exclude_list.append(False)
            continue

        penalty_type = verify_penalty(row["game_id"], row["time"], game_plays, game_shifts)
        if penalty_type == "Penalty":
            exclude_list.append(True)
        elif penalty_type == "Offsetting":
            exclude_list.append(False)
        else:
            exclude = (row["team_1"] != row["team_2"]) & (row["team_1"] <= 6) & (row["team_2"] <= 6)
            exclude_list.append(exclude)

    df_exclude["exclude"] = exclude_list
    return df_exclude.reset_index(drop=True)



def calculate_season_penalty_exclude_times(game_shifts, game_skater_stats, game_plays, game_ids):
    """
    Calculate penalty exclude times for all games in a season.

    Args:
        game_shifts (pd.DataFrame): Shift data for all games.
        game_skater_stats (pd.DataFrame): Skater stats data for all games.
        game_plays (pd.DataFrame): Play-by-play data for all games.
        game_ids (list): List of game IDs for the season.

    Returns:
        pd.DataFrame: Combined penalty exclude times for all games in the season.
    """
    all_exclude_times = []

    for game_id in game_ids:
        logging.info(f"Processing penalty exclude times for game_id: {game_id}")

        # Filter data for the specific game
        shifts = game_shifts[game_shifts["game_id"] == game_id]
        skater_stats = game_skater_stats[game_skater_stats["game_id"] == game_id]
        plays = game_plays[game_plays["game_id"] == game_id]

        if shifts.empty or skater_stats.empty or plays.empty:
            logging.warning(f"Skipping game_id {game_id} due to missing data.")
            continue

        # Calculate penalty exclude times
        exclude_times = get_penalty_exclude_times(shifts, skater_stats, plays)
        all_exclude_times.append(exclude_times)

    # Combine all results into a single DataFrame
    combined_exclude_times = pd.concat(all_exclude_times, ignore_index=True)
    return combined_exclude_times

def organize_by_season(df_master, season):
    """
    Organize the data for a specific season.

    Args:
        df_master (dict): A dictionary containing all game data (e.g., game_plays, game_shifts, game_skater_stats).
        season (int): The season to organize (e.g., 20152016).

    Returns:
        dict: A dictionary with filtered data for the specified season.
    """
    logging.info(f"Organizing data for season {season}.")

    # Filter game IDs for the specified season
    game_plays_season = df_master["game_plays"][df_master["game_plays"]["season"] == season]
    game_ids_season = game_plays_season["game_id"].unique()

    # Filter all DataFrames in df_master for the specified season
    organized_data = {
        name: df[df["game_id"].isin(game_ids_season)].copy()
        for name, df in df_master.items()
        if "game_id" in df.columns
    }

    logging.info(f"Data organized for season {season} with {len(game_ids_season)} games.")
    return organized_data, game_ids_season


if __name__ == "__main__":
    from load_data import get_env_vars, load_data
    # Load environment variables and data
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    # Specify the season you want to process
    season = 20152016

    # Organize data for the season
    organized_data, season_games = organize_by_season(df_master, season)

    # Calculate penalty exclude times for all games in the season
    penalty_exclude_times = calculate_season_penalty_exclude_times(
        game_shifts=organized_data["game_shifts"],
        game_skater_stats=organized_data["game_skater_stats"],
        game_plays=organized_data["game_plays"],
        game_ids=season_games,
    )

    # Save the results to a CSV file
    penalty_exclude_times.to_csv(f"penalty_exclude_times_{season}.csv", index=False)
    logging.info(f"Penalty exclude times saved to penalty_exclude_times_{season}.csv.")

    # Insert into database
    engine = get_db_engine(env_vars)
    with engine.connect() as connection:
        penalty_exclude_times.to_sql(
            "penalty_exclude_times",
            connection,
            if_exists="replace",
            index=False,
            method="multi",
        )
        logging.info("Penalty exclude times successfully inserted into the database.")