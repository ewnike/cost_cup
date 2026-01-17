"""
Helper function.

Author: Eric Winiecke
Date: May 5, 2025

"""

from constants import (
    S3_BUCKET_NAME,
    local_download_path,
    local_download_path_II,
    local_download_path_III,
    local_extract_path,
    local_extract_path_II,
    local_extract_path_III,
)
from db_utils import (
    define_game_plays_players,
    define_game_plays_processor,
    define_game_shifts_table,
    define_game_skater_stats,
    define_game_table,
    define_player_info_table,
    define_raw_pbp_table,
    define_raw_shifts_table,
    get_db_engine,
)

# 🔹 Get shared engine once
engine = get_db_engine()


# ------------------------------------------------------------------
# PANDAS dtype mappings keyed by table name
# ------------------------------------------------------------------
COLUMN_MAPPINGS: dict[str, dict[str, str]] = {
    # ----------------- game_skater_stats --------------------
    "game_skater_stats": {
        "game_id": "int64",
        "player_id": "int64",
        "team_id": "int64",
        "timeOnIce": "int64",
        "assists": "int64",
        "goals": "int64",
        "shots": "int64",
        "hits": "int64",
        "powerPlayGoals": "int64",
        "powerPlayAssists": "int64",
        "penaltyMinutes": "int64",
        "faceOffWins": "int64",
        "faceoffTaken": "int64",
        "takeaways": "int64",
        "giveaways": "int64",
        "shortHandedGoals": "int64",
        "shortHandedAssists": "int64",
        "blocked": "int64",
        "plusMinus": "int64",
        "evenTimeOnIce": "int64",
        "shortHandedTimeOnIce": "int64",
        "powerPlayTimeOnIce": "int64",
    },
    # ---------------------- game_table ----------------------
    "game_table": {
        "game_id": "int64",
        "season": "int64",
        "type": "string",
        "date_time_GMT": "datetime64[ns]",
        "away_team_id": "int64",
        "home_team_id": "int64",
        "away_goals": "int64",
        "home_goals": "int64",
        "outcome": "string",
        "home_rink_side_start": "string",
        "venue": "string",
        "venue_link": "string",
        "venue_time_zone_id": "string",
        "venue_time_zone_offset": "int64",
        "venue_time_zone_tz": "string",
    },
    # ------------------- game_shifts ------------------------
    "game_shifts": {
        "game_id": "int64",
        "player_id": "int64",
        "period": "int64",
        "shift_start": "int64",
        "shift_end": "int64",
    },
    # --------------- game_plays_processor -------------------
    "game_plays": {
        "play_id": "string",
        "game_id": "int64",
        "team_id_for": "int64",
        "team_id_against": "int64",
        "event": "string",
        "secondaryType": "string",
        "x": "float64",
        "y": "float64",
        "period": "int64",
        "periodType": "string",
        "periodTime": "int64",
        "periodTimeRemaining": "int64",
        "dateTime": "datetime64[ns]",
        "goals_away": "int64",
        "goals_home": "int64",
        "description": "string",
        "st_x": "int64",
        "st_y": "int64",
    },
    # ---------------- game_plays_players --------------------
    "game_plays_players": {
        "play_id": "string",
        "game_id": "int64",
        "player_id": "int64",
        "playerType": "string",
    },
    # ---------------- player_info_table ---------------------
    "player_info": {
        "player_id": "int64",
        "firstName": "string",
        "lastName": "string",
        "nationality": "string",
        "birthCity": "string",
        "primaryPosition": "string",
        "birthDate": "datetime64[ns]",
        "birthStateProvince": "string",
        "height": "float64",  # inches
        "height_cm": "float64",  # centimetres
        "weight": "float64",
        "shootCatches": "string",
    },
    # ---------------- raw_corsi_{season} -------------------------
    "raw_corsi": {
        "game_id": "int64",
        "player_id": "int64",
        "team_id": "int64",
        "corsi_for": "float64",
        "corsi_against": "float64",
        "corsi": "float64",
        "CF_Percent": "float64",
    },
    # ----------------- cap_hit -----------------------------------
    "cap_hit": {
        "firstName": "string",
        "lastName": "string",
        "capHit": "float64",
    },
    # --------------------- team_event_totals -----------------------
    "team_event_totals_games": {
        "team_id": "int64",
        "total_goals": "int64",
        "total_shots": "int64",
        "total_missed_shots": "int64",
        "total_blocked_shots_for": "int64",
        "total_goals_against": "int64",
        "total_shots_against": "int64",
        "total_missed_shots_against": "int64",
        "total_blocked_shots_against": "int64",
        "game_id": "int64",
    },
    # ----------------------- raw_shifts_{season} -------------------------------
    "raw_shifts": {
        "row_num": "int64",
        "player": "string",
        "team_num": "string",
        "position": "string",
        "game_id": "int64",
        "game_date": "datetime64[ns]",
        "season": "int64",
        "session": "string",
        "team": "string",
        "opponent": "string",
        "is_home": "int64",
        "game_period": "int64",
        "shift_num": "float64",
        "seconds_start": "int64",
        "seconds_end": "int64",
        "seconds_duration": "int64",
        "shift_start": "string",
        "shift_end": "string",
        "duration": "string",
        "shift_mod": "int64",
    },
    # --------------- pbp_raw_{season} ------------------
    "pbp_raw_data": {
        "season": "int64",
        "game_id": "int64",
        "game_date": "datetime64[ns]",
        "session": "string",
        "event_index": "int64",
        "game_period": "int64",
        "game_seconds": "int64",
        "clock_time": "string",
        "event_type": "string",
        "event_description": "string",
        "event_detail": "string",
        "event_zone": "string",
        "event_team": "string",
        "event_player_1": "string",
        "event_player_2": "string",
        "event_player_3": "string",
        "event_length": "int64",
        "coords_x": "float64",
        "coords_y": "float64",
        "num_on": "float64",
        "num_off": "float64",
        "players_on": "string",
        "players_off": "string",
        "home_on_1": "string",
        "home_on_2": "string",
        "home_on_3": "string",
        "home_on_4": "string",
        "home_on_5": "string",
        "home_on_6": "string",
        "home_on_7": "string",
        "away_on_1": "string",
        "away_on_2": "string",
        "away_on_3": "string",
        "away_on_4": "string",
        "away_on_5": "string",
        "away_on_6": "string",
        "away_on_7": "string",
        "home_goalie": "string",
        "away_goalie": "string",
        "home_team": "string",
        "away_team": "string",
        "home_skaters": "int64",
        "away_skaters": "int64",
        "home_score": "int64",
        "away_score": "int64",
        "game_score_state": "string",
        "game_strength_state": "string",
        "home_zone": "string",
        "pbp_distance": "float64",
        "event_distance": "float64",
        "event_angle": "float64",
        "home_zonestart": "float64",
        "face_index": "int64",
        "pen_index": "int64",
        "shift_index": "int64",
        "pred_goal": "float64",
    },
}


# pylint: disable=too-many-arguments
def build_processing_config(
    *,
    bucket_name,
    s3_file_key,
    season: int | str,
    local_zip_path,
    local_extract_path,
    expected_csv_filename,
    table_definition_function,
    table_name,
    column_mapping,
    engine,
    local_download_path,
):
    """
    Build a standardized config dictionary for S3 extraction and data processing.

    Args:
    ----
        bucket_name (str): Name of the S3 bucket.
        s3_file_key (str): Key to the ZIP file in the S3 bucket.
        local_zip_path (str): Local path to download the ZIP file.
        local_extract_path (str): Local path to extract the CSV contents.
        expected_csv_filename (str): Name of the expected CSV file inside the ZIP.
        table_definition_function (Callable): SQLAlchemy function to define the table.
        table_name (str): Name of the PostgreSQL table to insert into.
        column_mapping (dict): Column names and types for cleaning.
        engine (sqlalchemy.Engine): SQLAlchemy engine instance for database connection.
        local_download_path (str): Directory for downloading the ZIP file.

    Returns:
    -------
        dict: Config dictionary with all values needed for process_and_insert_data().

    """
    s3_file_key = s3_file_key.format(season=season)

    return {
        "bucket_name": bucket_name,
        "s3_file_key": s3_file_key,
        "local_zip_path": local_zip_path,
        "local_extract_path": local_extract_path,
        "expected_csv_filename": expected_csv_filename,
        "table_definition_function": table_definition_function,
        "table_name": table_name,
        "column_mapping": column_mapping,
        "engine": engine,
        "handle_zip": str(local_zip_path).lower().endswith(".zip"),
        "local_download_path": local_download_path,
    }


def game_skater_stats_config():
    """Predefined config for game skater stats."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="game_skater_stats.csv.zip",
        local_zip_path=f"{local_download_path}/game_skater_stats.zip",
        local_extract_path=local_extract_path,
        expected_csv_filename="game_skater_stats.csv",
        table_definition_function=define_game_skater_stats,
        table_name="game_skater_stats",
        column_mapping=COLUMN_MAPPINGS["game_skater_stats"],
        engine=engine,
        local_download_path=local_download_path,
    )


def game_table_config():
    """Predefined config for game table."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="game.csv.zip",
        local_zip_path=f"{local_download_path}/game.zip",
        local_extract_path=local_extract_path,
        expected_csv_filename="game.csv",
        table_definition_function=define_game_table,
        table_name="game",
        column_mapping=COLUMN_MAPPINGS["game_table"],
        engine=engine,
        local_download_path=local_download_path,
    )


def game_shifts_config():
    """Predefined config for game shifts."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="game_shifts.csv.zip",
        local_zip_path=f"{local_download_path}/game_shifts.zip",
        local_extract_path=local_extract_path,
        expected_csv_filename="game_shifts.csv",
        table_definition_function=define_game_shifts_table,
        table_name="game_shifts",
        column_mapping=COLUMN_MAPPINGS["game_shifts"],
        engine=engine,
        local_download_path=local_download_path,
    )


def game_plays_config():
    """Predefined config for game plays."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="game_plays.csv.zip",
        local_zip_path=f"{local_download_path}/game_plays.zip",
        local_extract_path=local_extract_path,
        expected_csv_filename="game_plays.csv",
        table_name="game_plays",
        table_definition_function=define_game_plays_processor,
        # table_name="game_plays_processor",
        column_mapping=COLUMN_MAPPINGS["game_plays"],
        engine=engine,
        local_download_path=local_download_path,
    )


def game_plays_players_config():
    """Predefined config for game plays players."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="game_plays_players.csv.zip",
        local_zip_path=f"{local_download_path}/game_plays_players.zip",
        local_extract_path=local_extract_path,
        expected_csv_filename="game_plays_players.csv",
        table_definition_function=define_game_plays_players,
        table_name="game_plays_players",
        column_mapping=COLUMN_MAPPINGS["game_plays_players"],
        engine=engine,
        local_download_path=local_download_path,
    )


def player_info_config():
    """Predefined config for player info."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="player_info.csv.zip",
        local_zip_path=f"{local_download_path}/player_info.zip",
        local_extract_path=local_extract_path,
        expected_csv_filename="player_info.csv",
        table_definition_function=define_player_info_table,
        table_name="player_info",
        column_mapping=COLUMN_MAPPINGS["player_info"],
        engine=engine,
        local_download_path=local_download_path,
    )


def raw_shifts_config(season: int):
    """Predefined config for raw shifts data."""
    table_name = f"raw_shifts_{season}"
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="shifts_{season}.csv.zip",
        season=season,
        local_zip_path=f"{local_download_path_III}/raw_shifts_{season}.zip",
        local_extract_path=local_extract_path_III,
        expected_csv_filename=f"shifts_{season}.csv",
        table_name=table_name,
        column_mapping=COLUMN_MAPPINGS["raw_shifts"],
        engine=engine,
        local_download_path=local_download_path_III,
        table_definition_function=lambda md: define_raw_shifts_table(md, f"raw_shifts_{season}"),
    )


def pbp_raw_data_config(season: int):
    """Predefined config for raw pbp game data."""
    table_name = f"raw_pbp_{season}"
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="pbp_{season}.csv.zip",
        season=season,
        local_zip_path=f"{local_download_path_II}/pbp_{season}.zip",
        local_extract_path=local_extract_path_II,
        expected_csv_filename=f"pbp_{season}.csv",
        table_name=table_name,
        column_mapping=COLUMN_MAPPINGS["pbp_raw_data"],
        engine=engine,
        local_download_path=local_download_path_II,
        table_definition_function=lambda md: define_raw_pbp_table(md, table_name),
        # handle_zip=True,
    )
