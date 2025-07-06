"""
Helper function.

Author: Eric Winiecke
Date: May 5, 2025

"""

from constants import S3_BUCKET_NAME, local_download_path, local_extract_path
from db_utils import (
    define_game_plays_players_test,
    define_game_plays_processor_test,
    define_game_shifts_test_table,
    define_game_skater_stats_test,
    define_game_table_test,
    define_player_info_table_test,
    get_db_engine,
)

# 🔹 Get shared engine once
engine = get_db_engine()


# ------------------------------------------------------------------
# PANDAS dtype mappings keyed by table name
# ------------------------------------------------------------------
COLUMN_MAPPINGS: dict[str, dict[str, str]] = {
    # ----------------- game_skater_stats_test --------------------
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
    # ---------------------- game_table_test ----------------------
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
    # ------------------- game_shifts_test ------------------------
    "game_shifts": {
        "game_id": "int64",
        "player_id": "int64",
        "period": "int64",
        "shift_start": "int64",
        "shift_end": "int64",
    },
    # --------------- game_plays_processor_test -------------------
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
    # ---------------- game_plays_players_test --------------------
    "game_plays_players": {
        "play_id": "string",
        "game_id": "int64",
        "player_id": "int64",
        "playerType": "string",
    },
    # ---------------- player_info_table_test ---------------------
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
}


# pylint: disable=too-many-arguments
def build_processing_config(
    *,
    bucket_name,
    s3_file_key,
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
        "handle_zip": bool(local_zip_path),
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
        table_definition_function=define_game_skater_stats_test,
        table_name="game_skater_stats_test",
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
        table_definition_function=define_game_table_test,
        table_name="game_table_test",
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
        table_definition_function=define_game_shifts_test_table,
        table_name="game_shifts_test",
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
        table_definition_function=define_game_plays_processor_test,
        table_name="game_plays_processor_test",
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
        table_definition_function=define_game_plays_players_test,
        table_name="game_plays_players_test",
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
        table_definition_function=define_player_info_table_test,
        table_name="player_info_table_test",
        column_mapping=COLUMN_MAPPINGS["player_info"],
        engine=engine,
        local_download_path=local_download_path,
    )
