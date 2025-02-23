"""
October 1, 2024.

Helper function for accessing
the database and reducing clutter from
redundant code.

Eric Winiecke
"""

import logging
import os

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)


def define_game_plays_processor_test(metadata):
    """Define the table schema for game_plays_processor_test."""
    return Table(
        "game_plays_processor_test",
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


def define_game_plays_players_test(metadata):
    """Define the table schema for game_plays_players_test."""
    return Table(
        "game_plays_players_test",
        metadata,
        Column("play_id", String(20)),
        Column("game_id", BigInteger, nullable=False),
        Column("player_id", BigInteger, nullable=False),
        Column("playerType", String(20)),
    )


def define_game_processor_test(metadata):
    """Define the table schema for game_processor_test."""
    return Table(
        "game_processor_test",
        metadata,
        Column("game_id", BigInteger, primary_key=True),
        Column("season", Integer),
        Column("type", String(20)),
        Column("date_time_GMT", DateTime(timezone=False)),
        Column("away_team_id", Integer),
        Column("home_team_id", Integer),
        Column("away_goals", Integer),
        Column("home_goals", Integer),
        Column("outcome", String(50)),
        Column("home_rink_side_start", String(20)),
        Column("venue", String(100)),
        Column("venue_link", String(255)),
        Column("venue_time_zone_id", String(50)),
        Column("venue_time_zone_offset", Integer),
        Column("venue_time_zone_tz", String(10)),
    )


def define_game_shifts_test_table(metadata):
    """Define the table schema for game_shifts_test."""
    return Table(
        "game_shifts_test",
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("period", Integer),
        Column("shift_start", Integer),
        Column("shift_end", Integer),
    )


def define_game_skater_stats_test(metadata):
    """Define the table schema for game_skater_stats_test."""
    return Table(
        "game_skater_stats_test",
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


# Add additional table definitions as needed


# Load environment variables from the .env file
def load_environment_variables():
    """
    Loads environment variables from the `.env` file into the system environment.

    This function uses the `load_dotenv` library to read key-value pairs from an `.env` file
    and set them as environment variables in the application.

    Returns:
    -------
        None

    Example:
    -------
        >>> load_environment_variables()
        # Environment variables are now available via os.getenv("VAR_NAME")

    """  # noqa: D401
    load_dotenv()
    logging.info("Environment variables loaded.")  # Debug log to confirm loading


def get_db_engine():
    """
    Creates and returns a SQLAlchemy database engine using environment variables.

    This function retrieves database connection parameters from environment variables
    and constructs a SQLAlchemy connection engine.

    Returns:
    -------
        sqlalchemy.engine.Engine: A SQLAlchemy database engine instance.

    Raises:
    ------
        ValueError: If any required environment variables are missing.

    Environment Variables:
        - DATABASE_TYPE: The type of database (e.g., "postgresql").
        - DBAPI: The database API to use (e.g., "psycopg2").
        - ENDPOINT: The database server address.
        - USER: The database username.
        - PASSWORD: The database password.
        - PORT: The port to connect to (default: 5432).
        - DATABASE: The name of the database.

    Example:
    -------
        >>> engine = get_db_engine()
        >>> connection = engine.connect()

    """  # noqa: D401
    # Load the environment variables
    load_environment_variables()

    # Retrieve database connection parameters from environment variables
    # pylint: disable=invalid-name
    DATABASE_TYPE = os.getenv("DATABASE_TYPE")
    DBAPI = os.getenv("DBAPI")
    ENDPOINT = os.getenv("ENDPOINT")
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    PORT = os.getenv("PORT", 5432)
    DATABASE = os.getenv("DATABASE")
    # pylint: enable=invalid-name  # Re-enable the rule after this block

    # Check if required environment variables are loaded
    if not all([DATABASE_TYPE, DBAPI, ENDPOINT, USER, PASSWORD, DATABASE]):
        logging.error("Missing one or more required environment variables.")
        raise ValueError("Missing one or more required environment variables.")

    # Create the connection string
    connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
    logging.info(f"Database connection string created: {connection_string}")

    # Return the SQLAlchemy engine
    return create_engine(connection_string)


# Function to return SQLAlchemy metadata
def get_metadata():
    """
    Returns a SQLAlchemy MetaData instance.

    The MetaData object is used to manage database schema information within SQLAlchemy.
    It acts as a registry of tables and allows for schema reflection.

    Returns:
    -------
        sqlalchemy.MetaData: A MetaData instance.

    Example:
    -------
        >>> metadata = get_metadata()
        >>> metadata.reflect(bind=engine)  # Reflect existing database schema

    """  # noqa: D401
    return MetaData()


def create_table(engine, metadata):
    """
    Create all tables defined in the given SQLAlchemy MetaData object.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy database engine instance.
    metadata : sqlalchemy.MetaData
        The SQLAlchemy MetaData instance that holds the table definitions.

    Returns
    -------
    None

    """
    metadata.create_all(engine)
    logging.info("Database tables created or verified.")
