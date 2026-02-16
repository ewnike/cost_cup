"""
db_utils.py.

Helper functions for database access and table creation.

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os
import pathlib
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.schema import Identity

from log_utils import setup_logger

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

DERIVED_SCHEMA = "derived"  # you already have this constant
RAW_SCHEMA = "raw"

setup_logger()
logger = logging.getLogger(__name__)  # ✅ define logger


# 🔹 **Step 1: Load Environment Variables**
# def load_environment_variables():
#     """Load environment variables from the .env next to this file."""
#     env_path = Path(__file__).resolve().parent / ".env"
#     load_dotenv(dotenv_path=env_path, override=False)
#     logger.info("Environment variables loaded.")


def load_environment_variables():
    """Load environment variables from the .env next to this file."""
    env_path = Path(__file__).resolve().parent / ".env"
    print("Loading .env from:", env_path)  # TEMP
    load_dotenv(dotenv_path=env_path, override=False)


# 🔹 **Step 2: Get Database Engine**
# def get_db_engine():
#     """
#     Create and return a SQLAlchemy database engine.

#     - Uses `DATABASE_URL` if available.
#     - Otherwise, constructs a connection string from individual environment variables.

#     Returns
#     -------
#         sqlalchemy.engine.Engine: A SQLAlchemy database engine instance.

#     Raises
#     ------
#         ValueError: If `DATABASE_URL` is missing and required variables are not set.

#     Environment Variables:
#         - DATABASE_URL (optional, takes priority if set)
#         - DATABASE_TYPE
#         - DBAPI
#         - ENDPOINT
#         - USER
#         - PASSWORD
#         - PORT (default: 5432)
#         - DATABASE

#     """
#     load_environment_variables()  # Ensure variables are loaded

#     # Check if DATABASE_URL is set
#     # pylint: disable=invalid-name
#     DATABASE_URL = os.getenv("DATABASE_URL")

#     if DATABASE_URL:
#         logger.info("Using DATABASE_URL from environment.")
#         return create_engine(DATABASE_URL)

#     # Otherwise, construct from individual variables

#     DATABASE_TYPE = os.getenv("DATABASE_TYPE")
#     DBAPI = os.getenv("DBAPI")
#     ENDPOINT = os.getenv("ENDPOINT")
#     USER = os.getenv("DB_USER")
#     PASSWORD = os.getenv("DB_PASSWORD")
#     PORT = os.getenv("PORT", "5432")  # Default PostgreSQL port
#     DATABASE = os.getenv("DATABASE")

#     # pylint: enable=invalid-name
#     # Ensure all required variables are available
#     missing_vars = [
#         var
#         for var in ["DATABASE_TYPE", "DBAPI", "ENDPOINT", "DB_USER", "DB_PASSWORD", "DATABASE"]
#         if not os.getenv(var)
#     ]
#     if missing_vars:
#         logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
#         raise ValueError("ERROR: One or more required environment variables are missing.")

#     # URL encode the password in case it contains special characters
#     encoded_password = quote_plus(PASSWORD)

#     # Create the connection string
#     connection_string = (
#         f"{DATABASE_TYPE}+{DBAPI}://{USER}:{encoded_password}@{ENDPOINT}:{PORT}/{DATABASE}"
#     )
#     logger.info("Database connection string created.")


#     return create_engine(connection_string)
def get_db_engine():
    """
    Create and return a SQLAlchemy database engine.

    Priority:
      1) DATABASE_URL (if set)
      2) If APP_ENV=aws, use AWS_DB_* variables
      3) Otherwise use local DB_* variables

    Local env vars:
      DATABASE_TYPE, DBAPI, ENDPOINT, PORT, DATABASE, DB_USER, DB_PASSWORD, SSL_MODE(optional)

    AWS env vars:
      AWS_DB_HOST, AWS_DB_PORT, AWS_DB_NAME, AWS_DB_USER, AWS_DB_PASSWORD, AWS_DB_SSLMODE(optional)
    """
    load_environment_variables()

    database_url = os.getenv("DATABASE_URL")
    if database_url:
        logger.info("Using DATABASE_URL from environment.")
        return create_engine(database_url, pool_pre_ping=True)

    database_type = os.getenv("DATABASE_TYPE")
    dbapi = os.getenv("DBAPI")
    app_env = (os.getenv("APP_ENV") or "local").strip().lower()

    if not database_type or not dbapi:
        raise ValueError("Missing DATABASE_TYPE or DBAPI in environment.")

    if app_env == "aws":
        host = os.getenv("AWS_DB_HOST")
        port = os.getenv("AWS_DB_PORT", "5432")
        dbname = os.getenv("AWS_DB_NAME")
        user = os.getenv("AWS_DB_USER")
        password = os.getenv("AWS_DB_PASSWORD")
        ssl_mode = os.getenv("AWS_DB_SSLMODE") or os.getenv("SSL_MODE") or "require"

        missing = [
            k
            for k, v in {
                "AWS_DB_HOST": host,
                "AWS_DB_NAME": dbname,
                "AWS_DB_USER": user,
                "AWS_DB_PASSWORD": password,
            }.items()
            if not v
        ]

        if missing:
            logger.error("Missing required AWS env vars: %s", ", ".join(missing))
            raise ValueError("Missing required AWS database environment variables.")

        logger.info(
            "Using AWS database config (APP_ENV=aws). host=%s db=%s port=%s", host, dbname, port
        )

    else:
        host = os.getenv("ENDPOINT")
        port = os.getenv("PORT", "5432")
        dbname = os.getenv("DATABASE")
        user = os.getenv("DB_USER") or os.getenv("USER")
        password = os.getenv("DB_PASSWORD") or os.getenv("PASSWORD")
        ssl_mode = os.getenv("SSL_MODE")  # optional

        missing = [
            k
            for k, v in {
                "ENDPOINT": host,
                "DATABASE": dbname,
                "DB_USER (or USER)": user,
                "DB_PASSWORD (or PASSWORD)": password,
            }.items()
            if not v
        ]

        if missing:
            logger.error("Missing required local env vars: %s", ", ".join(missing))
            raise ValueError("Missing required local database environment variables.")

        logger.info(
            "Using local database config (APP_ENV=%s). host=%s db=%s port=%s",
            app_env,
            host,
            dbname,
            port,
        )

    encoded_password = quote_plus(password)
    connection_string = (
        f"{database_type}+{dbapi}://{user}:{encoded_password}@{host}:{port}/{dbname}"
    )

    connect_args = {}
    if ssl_mode:
        connect_args["sslmode"] = ssl_mode

    return create_engine(
        connection_string,
        connect_args=connect_args,
        pool_pre_ping=True,
    )


# 🔹 **Step 3: Global MetaData Object (Prevents Duplication Issues)**
metadata = MetaData()


def get_metadata():
    """Return a global SQLAlchemy MetaData object."""
    return metadata


# 🔹 **Step 4: Table Definitions**
def define_game_skater_stats(metadata):
    """Define and return the schema for game_skater_stats."""
    return Table(
        "game_skater_stats",
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("team_id", Integer),
        Column("timeOnIce", Integer),
        Column("assists", Integer),
        Column("goals", Integer),
        Column("shots", Integer),
        Column("hits", Integer),
        Column("powerPlayGoals", Integer),
        Column("powerPlayAssists", Integer),
        Column("penaltyMinutes", Integer),
        Column("faceOffWins", Integer),
        Column("faceoffTaken", Integer),
        Column("takeaways", Integer),
        Column("giveaways", Integer),
        Column("shortHandedGoals", Integer),
        Column("shortHandedAssists", Integer),
        Column("blocked", Integer),
        Column("plusMinus", Integer),
        Column("evenTimeOnIce", Integer),
        Column("shortHandedTimeOnIce", Integer),
        Column("powerPlayTimeOnIce", Integer),
    )


def define_game_table(metadata):
    """Define the game table schema."""
    return Table(
        "game",
        metadata,
        Column("game_id", Integer),
        Column("season", Integer),
        Column("type", String),
        Column("date_time_GMT", DateTime),
        Column("away_team_id", Integer),
        Column("home_team_id", Integer),
        Column("away_goals", Integer),
        Column("home_goals", Integer),
        Column("outcome", String),
        Column("home_rink_side_start", String),
        Column("venue", String),
        Column("venue_link", String),
        Column("venue_time_zone_id", String),
        Column("venue_time_zone_offset", Integer),
        Column("venue_time_zone_tz", String),
    )


def define_game_shifts_table(metadata):
    """Define the table schema for game_shifts_test."""
    return Table(
        "game_shifts",
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("period", Integer),
        Column("shift_start", Integer),
        Column("shift_end", Integer),
    )


def define_game_plays_processor(metadata):
    """Define the schema for game_plays and return the table."""
    return Table(
        "game_plays",
        metadata,
        Column("play_id", String(20), primary_key=True),
        Column("game_id", BigInteger),
        Column("team_id_for", Integer, nullable=True),
        Column("team_id_against", Integer, nullable=True),
        Column("event", String(50)),
        Column("secondaryType", String(50)),
        Column("x", Float, nullable=True),
        Column("y", Float, nullable=True),
        Column("period", Integer),
        Column("periodType", String(50)),
        Column("periodTime", Integer),
        Column("periodTimeRemaining", Integer),
        Column("dateTime", DateTime(timezone=False)),
        Column("goals_away", Integer, nullable=True),
        Column("goals_home", Integer, nullable=True),
        Column("description", String(255)),
        Column("st_x", Integer, nullable=True),
        Column("st_y", Integer, nullable=True),
    )


def define_game_plays_players(metadata):
    """Define and return the schema for game_plays_players."""
    return Table(
        "game_plays_players",
        metadata,
        Column("play_id", String(20)),
        Column("game_id", BigInteger, nullable=False),
        Column("player_id", BigInteger, nullable=False),
        Column("playerType", String(20)),
    )


def define_player_info_table(metadata):
    """Define the player_info table schema."""
    return Table(
        "player_info",
        metadata,
        Column("player_id", BigInteger, primary_key=True),  # Unique player identifier
        Column("firstName", String(50)),
        Column("lastName", String(50)),
        Column("nationality", String(50)),
        Column("birthCity", String(50)),
        Column("primaryPosition", String(50)),
        Column("birthDate", DateTime),  # Date of birth
        Column("birthStateProvince", String(50)),
        Column("height", Float),  # Height in inches
        Column("height_cm", Float),  # Height in centimeters
        Column("weight", Float),  # Weight in pounds
        Column("shootCatches", String(10)),  # Shooting/Catching hand
    )


def define_raw_corsi_table(metadata, table_name: str = "raw_corsi"):
    """Define the raw corsi table schema."""
    return Table(
        table_name,
        metadata,
        Column("game_id", Integer),
        Column("player_id", Integer),
        Column("team_id", Integer),
        Column("corsi_for", Integer),
        Column("corsi_against", Integer),
        Column("corsi", Integer),
        Column("cf_percent", Float),
        extend_existing=True,
    )


def define_raw_shifts_table(metadata, table_name: str = "raw_shifts"):
    """Define the player raw shifts table schema."""
    return Table(
        table_name,
        metadata,
        Column("id", BigInteger, Identity(always=False), primary_key=True),
        Column("row_num", Integer, nullable=True),
        Column("player", Text, nullable=False),
        Column("team_num", String(10), nullable=False),
        Column("position", String(1), nullable=False),
        Column("game_id", BigInteger, nullable=False),
        Column("game_date", Date, nullable=False),
        Column("season", Integer, nullable=False),
        Column("session", String(1), nullable=False),
        Column("team", String(3), nullable=False),
        Column("opponent", String(3), nullable=False),
        Column("is_home", Boolean, nullable=False),
        Column("game_period", SmallInteger, nullable=False),
        Column("shift_num", Integer, nullable=False),  # it shows as a float but should be int
        Column("seconds_start", Integer, nullable=False),  # seconds start at 0 go up 3600+ in OT
        Column("seconds_end", Integer, nullable=False),
        Column("seconds_duration", Integer, nullable=False),
        Column("shift_start", String(5), nullable=True),
        Column("shift_end", String(5), nullable=True),
        Column("duration", String(5), nullable=True),
        Column("shift_mod", Integer, nullable=False),
        extend_existing=True,
    )


def define_raw_pbp_table(metadata, table_name: str = "raw_pbp"):
    """Define the game raw pbp table schema."""
    return Table(
        table_name,
        metadata,
        Column("id", BigInteger, Identity(always=False), primary_key=True),
        Column("season", Integer, nullable=False),
        Column("game_id", BigInteger, nullable=False),
        Column("game_date", Date, nullable=False),
        Column("session", String(1), nullable=False),
        Column("event_index", Integer),
        Column("game_period", SmallInteger),
        Column("game_seconds", Integer),
        Column("clock_time", String(5)),  # "mm:ss"
        Column("event_type", String(50)),
        Column("event_description", Text),
        Column("event_detail", Text),
        Column("event_zone", String(20)),
        Column("event_team", String(3)),  # e.g. MTL/TOR
        Column("event_player_1", Text),
        Column("event_player_2", Text),
        Column("event_player_3", Text),
        Column("event_length", Integer),
        Column("coords_x", Float),
        Column("coords_y", Float),
        Column("num_on", Float),  # keep float if you can have NaN
        Column("num_off", Float),
        Column("players_on", Text),
        Column("players_off", Text),
        Column("home_on_1", Text),
        Column("home_on_2", Text),
        Column("home_on_3", Text),
        Column("home_on_4", Text),
        Column("home_on_5", Text),
        Column("home_on_6", Text),
        Column("home_on_7", Text),
        Column("away_on_1", Text),
        Column("away_on_2", Text),
        Column("away_on_3", Text),
        Column("away_on_4", Text),
        Column("away_on_5", Text),
        Column("away_on_6", Text),
        Column("away_on_7", Text),
        Column("home_goalie", Text),
        Column("away_goalie", Text),
        Column("home_team", String(3)),
        Column("away_team", String(3)),
        Column("home_skaters", SmallInteger),
        Column("away_skaters", SmallInteger),
        Column("home_score", SmallInteger),
        Column("away_score", SmallInteger),
        Column("game_score_state", String(20)),
        Column("game_strength_state", String(20)),
        Column("home_zone", String(20)),
        Column("pbp_distance", Float),
        Column("event_distance", Float),
        Column("event_angle", Float),
        Column("home_zonestart", Float),
        Column("face_index", Integer),
        Column("pen_index", Integer),
        Column("shift_index", Integer),
        Column("pred_goal", Float),
        extend_existing=True,
    )


def create_corsi_table(table_name: str, metadata: MetaData) -> Table:
    """
    Dynamically define and return a Corsi table with the given name.

    Parameters
    ----------
    table_name : str
        The name to assign to the Corsi table.
    metadata : sqlalchemy.MetaData
        The shared metadata object.

    Returns
    -------
    sqlalchemy.Table
        SQLAlchemy table object.

    """
    return Table(
        table_name,
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("team_id", Integer),
        Column("corsi_for", Float),
        Column("corsi_against", Float),
        Column("corsi", Float),
        Column("cf_percent", Float),
        extend_existing=True,
    )


def create_caphit_table(table_name: str, metadata: MetaData) -> Table:
    """Cap-hit by season with player_id for reliable joins."""
    t = Table(
        table_name,
        metadata,
        Column("player_id", BigInteger, nullable=True),  # allow null while resolving
        Column("firstName", String(50), nullable=True),
        Column("lastName", String(50), nullable=True),
        Column("capHit", Float, nullable=True),
        Column("spotrac_url", String(255), nullable=True),
        extend_existing=True,
    )
    Index(f"ix_{table_name}_player_id", t.c.player_id)
    Index(f"ix_{table_name}_spotrac_url", t.c.spotrac_url)
    return t


def create_team_event_total_games_table(table_name: str, metadata: MetaData) -> Table:
    """Define team event totals schema."""
    return Table(
        table_name,
        metadata,
        Column("team_id", Integer),
        Column("total_goals", Integer),
        Column("total_shots", Integer),
        Column("total_missed_shots", Integer),
        Column("total_blocked_shots_for", Integer),
        Column("total_goals_against", Integer),
        Column("total_shots_against", Integer),
        Column("total_missed_shots_against", Integer),
        Column("total_blocked_shots_against", Integer),
        Column("game_id", BigInteger),
    )


def create_player_game_es_table(table_name: str, metadata: MetaData) -> Table:
    """
    Grain: one row per (game_id, player_id, team_id) for even-strength only.

    Stored per season as player_game_es_{season}.
    """
    t = Table(
        table_name,
        metadata,
        Column("game_id", BigInteger, nullable=False),
        Column("player_id", BigInteger, nullable=False),
        Column("team_id", Integer, nullable=False),
        # raw totals for that game at even strength
        Column("cf", Integer, nullable=False),
        Column("ca", Integer, nullable=False),
        Column("toi_sec", Integer, nullable=False),
        # derived rates (optional but handy)
        Column("cf60", Float),
        Column("ca60", Float),
        Column("cf_percent", Float),
        PrimaryKeyConstraint("game_id", "player_id", "team_id", name=f"pk_{table_name}"),
        extend_existing=True,
    )

    Index(f"ix_{table_name}_player", t.c.player_id)
    Index(f"ix_{table_name}_game", t.c.game_id)
    Index(f"ix_{table_name}_team", t.c.team_id)
    return t


def ctas_game_plays_from_raw_pbp(engine, season: int, *, drop: bool = True) -> str:
    """
    Build derived.game_plays_{season}_from_raw_pbp from raw.raw_pbp_{season}.

    Returns fully-qualified derived table name.
    """
    derived_table = f"game_plays_{season}_from_raw_pbp"
    raw_table = f"raw_pbp_{season}"

    # Drop view first (if it exists), then table.
    sql_drop_view = text(f'DROP VIEW IF EXISTS "{DERIVED_SCHEMA}"."{derived_table}" CASCADE;')
    sql_drop_table = text(f'DROP TABLE IF EXISTS "{DERIVED_SCHEMA}"."{derived_table}" CASCADE;')

    sql_ctas = text(
        f"""
        CREATE TABLE "{DERIVED_SCHEMA}"."{derived_table}" AS
        SELECT
          r.season,
          r.game_id,
          r.game_date,
          r.session,
          r.event_index,
          r.game_period,
          r.game_seconds,
          r.clock_time,
          r.event_type,
          r.event_description,
          r.event_detail,
          r.event_zone,
          r.event_team,
          r.event_player_1,
          r.event_player_2,
          r.event_player_3,
          r.home_team,
          r.away_team,
          r.home_skaters,
          r.away_skaters,
          r.home_score,
          r.away_score,
          r.game_score_state,
          r.game_strength_state,
          r.coords_x,
          r.coords_y,
          r.event_distance,
          r.event_angle,
          r.pred_goal
        FROM "{RAW_SCHEMA}"."{raw_table}" r
        WHERE r.season = :season
          AND r.session = 'R';
        """
    )

    sql_idx = text(
        f"""
        CREATE INDEX IF NOT EXISTS ix_{derived_table}_game_event
        ON "{DERIVED_SCHEMA}"."{derived_table}" (game_id, event_index);
        """
    )

    with engine.begin() as conn:
        if drop:
            conn.execute(sql_drop_view)
            conn.execute(sql_drop_table)
        conn.execute(sql_ctas, {"season": int(season)})
        conn.execute(sql_idx)

    return f"{DERIVED_SCHEMA}.{derived_table}"


def create_table(engine, metadata, table):
    """
    Create a specific table in the database.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The database engine.
    metadata : sqlalchemy.MetaData
        The SQLAlchemy MetaData instance.
    table : sqlalchemy.Table
        The table object to create.

    """
    metadata.create_all(engine, tables=[table])  # ✅ Create only the passed table
    logger.info(f"Table '{table.name}' created or verified.")
