# """
# October 1, 2024.

# Helper function for accessing
# the database and reducing clutter from
# redundant code.

# Eric Winiecke
# """

# import logging
# import os

# from dotenv import load_dotenv
# from sqlalchemy import (
#     BigInteger,
#     Column,
#     DateTime,
#     Float,
#     Integer,
#     MetaData,
#     String,
#     Table,
#     create_engine,
# )


# def define_game_plays_processor_test(metadata):
#     """Define the table schema for game_plays_processor_test."""
#     return Table(
#         "game_plays_processor_test",
#         metadata,
#         Column("play_id", String(20), primary_key=True),
#         Column("game_id", BigInteger),
#         Column("team_id_for", Integer, nullable=True),
#         Column("team_id_against", Integer, nullable=True),
#         Column("event", String(50)),
#         Column("secondaryType", String(50)),
#         Column("x", Float, nullable=True),
#         Column("y", Float, nullable=True),
#         Column("period", Integer),
#         Column("periodType", String(50)),
#         Column("periodTime", Integer),
#         Column("periodTimeRemaining", Integer),
#         Column("dateTime", DateTime(timezone=False)),
#         Column("goals_away", Integer, nullable=True),
#         Column("goals_home", Integer, nullable=True),
#         Column("description", String(255)),
#         Column("st_x", Integer, nullable=True),
#         Column("st_y", Integer, nullable=True),
#     )


# def define_game_plays_players_test(metadata):
#     """Define the table schema for game_plays_players_test."""
#     return Table(
#         "game_plays_players_test",
#         metadata,
#         Column("play_id", String(20)),
#         Column("game_id", BigInteger, nullable=False),
#         Column("player_id", BigInteger, nullable=False),
#         Column("playerType", String(20)),
#     )


# def define_game_processor_test(metadata):
#     """Define the table schema for game_processor_test."""
#     return Table(
#         "game_processor_test",
#         metadata,
#         Column("game_id", BigInteger, primary_key=True),
#         Column("season", Integer),
#         Column("type", String(20)),
#         Column("date_time_GMT", DateTime(timezone=False)),
#         Column("away_team_id", Integer),
#         Column("home_team_id", Integer),
#         Column("away_goals", Integer),
#         Column("home_goals", Integer),
#         Column("outcome", String(50)),
#         Column("home_rink_side_start", String(20)),
#         Column("venue", String(100)),
#         Column("venue_link", String(255)),
#         Column("venue_time_zone_id", String(50)),
#         Column("venue_time_zone_offset", Integer),
#         Column("venue_time_zone_tz", String(10)),
#     )


# def define_game_shifts_test_table(metadata):
#     """Define the table schema for game_shifts_test."""
#     return Table(
#         "game_shifts_test",
#         metadata,
#         Column("game_id", BigInteger),
#         Column("player_id", BigInteger),
#         Column("period", Integer),
#         Column("shift_start", Integer),
#         Column("shift_end", Integer),
#     )


# def define_game_skater_stats_test(metadata):
#     """Define the table schema for game_skater_stats_test."""
#     return Table(
#         "game_skater_stats_test",
#         metadata,
#         Column("game_id", BigInteger),
#         Column("player_id", BigInteger),
#         Column("team_id", Integer),
#         Column("timeOnIce", Integer),
#         Column("assists", Integer),
#         Column("goals", Integer),
#         Column("shots", Integer),
#         Column("hits", Integer),
#         Column("powerPlayGoals", Integer),
#         Column("powerPlayAssists", Integer),
#         Column("penaltyMinutes", Integer),
#         Column("faceOffWins", Integer),
#         Column("faceoffTaken", Integer),
#         Column("takeaways", Integer),
#         Column("giveaways", Integer),
#         Column("shortHandedGoals", Integer),
#         Column("shortHandedAssists", Integer),
#         Column("blocked", Integer),
#         Column("plusMinus", Integer),
#         Column("evenTimeOnIce", Integer),
#         Column("shortHandedTimeOnIce", Integer),
#         Column("powerPlayTimeOnIce", Integer),
#     )


# # Add additional table definitions as needed


# # Load environment variables from the .env file
# def load_environment_variables():
#     """
#     Loads environment variables from the `.env` file into the system environment.

#     This function uses the `load_dotenv` library to read key-value pairs from an `.env` file
#     and set them as environment variables in the application.

#     Returns:
#     -------
#         None

#     Example:
#     -------
#         >>> load_environment_variables()
#         # Environment variables are now available via os.getenv("VAR_NAME")

#     """  # noqa: D401
#     load_dotenv()
#     logging.info("Environment variables loaded.")  # Debug log to confirm loading


# def get_db_engine():
#     """
#     Creates and returns a SQLAlchemy database engine using environment variables.

#     This function retrieves database connection parameters from environment variables
#     and constructs a SQLAlchemy connection engine.

#     Returns:
#     -------
#         sqlalchemy.engine.Engine: A SQLAlchemy database engine instance.

#     Raises:
#     ------
#         ValueError: If any required environment variables are missing.

#     Environment Variables:
#         - DATABASE_TYPE: The type of database (e.g., "postgresql").
#         - DBAPI: The database API to use (e.g., "psycopg2").
#         - ENDPOINT: The database server address.
#         - USER: The database username.
#         - PASSWORD: The database password.
#         - PORT: The port to connect to (default: 5432).
#         - DATABASE: The name of the database.

#     Example:
#     -------
#         >>> engine = get_db_engine()
#         >>> connection = engine.connect()

#     """  # noqa: D401
#     # Load the environment variables
#     load_environment_variables()

#     # Retrieve database connection parameters from environment variables
#     # pylint: disable=invalid-name
#     DATABASE_TYPE = os.getenv("DATABASE_TYPE")
#     DBAPI = os.getenv("DBAPI")
#     ENDPOINT = os.getenv("ENDPOINT")
#     USER = os.getenv("USER")
#     PASSWORD = os.getenv("PASSWORD")
#     PORT = os.getenv("PORT", 5432)
#     DATABASE = os.getenv("DATABASE")
#     # pylint: enable=invalid-name  # Re-enable the rule after this block

#     # Check if required environment variables are loaded
#     if not all([DATABASE_TYPE, DBAPI, ENDPOINT, USER, PASSWORD, DATABASE]):
#         logging.error("Missing one or more required environment variables.")
#         raise ValueError("Missing one or more required environment variables.")

#     # Create the connection string
#     connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
#     logging.info(f"Database connection string created: {connection_string}")

#     # Return the SQLAlchemy engine
#     return create_engine(connection_string)


# # Function to return SQLAlchemy metadata
# def get_metadata():
#     """
#     Returns a SQLAlchemy MetaData instance.

#     The MetaData object is used to manage database schema information within SQLAlchemy.
#     It acts as a registry of tables and allows for schema reflection.

#     Returns:
#     -------
#         sqlalchemy.MetaData: A MetaData instance.

#     Example:
#     -------
#         >>> metadata = get_metadata()
#         >>> metadata.reflect(bind=engine)  # Reflect existing database schema

#     """  # noqa: D401
#     return MetaData()


# def create_table(engine, metadata):
#     """
#     Create all tables defined in the given SQLAlchemy MetaData object.

#     Parameters
#     ----------
#     engine : sqlalchemy.engine.Engine
#         The SQLAlchemy database engine instance.
#     metadata : sqlalchemy.MetaData
#         The SQLAlchemy MetaData instance that holds the table definitions.

#     Returns
#     -------
#     None

#     """
#     metadata.create_all(engine)
#     logging.info("Database tables created or verified.")


# """
# db_utils.py.

# Helper functions for database access and table creation.

# Author: Eric Winiecke
# Date: February 2025
# """

# import logging
# import os
# from urllib.parse import quote_plus

# from dotenv import load_dotenv
# from sqlalchemy import (
#     BigInteger,
#     Column,
#     DateTime,
#     Float,
#     Integer,
#     MetaData,
#     String,
#     Table,
#     create_engine,
# )


# def get_db_engine():
#     """
#     Create and return a SQLAlchemy database engine. Use either:
#     1. `DATABASE_URL` (if set) - a full connection string.
#     2. Individual environment variables to construct the connection string.

#     Returns:
#     -------
#         sqlalchemy.engine.Engine: A SQLAlchemy database engine instance.

#     Raises:
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

#     Example:
#     -------
#         >>> engine = get_db_engine()
#         >>> connection = engine.connect()

#     """
# Load environment variables from `.env`


#     load_dotenv()

#     # Try to get the full DATABASE_URL first
#     DATABASE_URL = os.getenv("DATABASE_URL")

#     if DATABASE_URL:
#         logging.info("Using DATABASE_URL from environment.")
#         return create_engine(DATABASE_URL)

#     # Otherwise, fall back to individual environment variables
#     DATABASE_TYPE = os.getenv("DATABASE_TYPE")
#     DBAPI = os.getenv("DBAPI")
#     ENDPOINT = os.getenv("ENDPOINT")
#     USER = os.getenv("USER")
#     PASSWORD = os.getenv("PASSWORD")
#     PORT = os.getenv("PORT", "5432")  # Default PostgreSQL port
#     DATABASE = os.getenv("DATABASE")

#     # Ensure all required variables are available
#     if not all([DATABASE_TYPE, DBAPI, ENDPOINT, USER, PASSWORD, DATABASE]):
#         missing_vars = [
#             var
#             for var in ["DATABASE_TYPE", "DBAPI", "ENDPOINT", "USER", "PASSWORD", "DATABASE"]
#             if os.getenv(var) is None
#         ]
#         logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
#         raise ValueError("ERROR: One or more required environment variables are missing.")

#     # URL encode the password in case it contains special characters
#     encoded_password = quote_plus(PASSWORD)

#     # Create the connection string
#     connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
#     logging.info(f"Database connection string created: {connection_string}")

#     # Return the SQLAlchemy engine
#     return create_engine(connection_string)


# # 🔹 **Step 1: Load environment variables**
# def load_environment_variables():
#     """
#     Load environment variables from the `.env` file.
#     Ensures that the database connection details are available.
#     """
#     load_dotenv()
#     logging.info("Environment variables loaded.")

#     # Load environment variables at import time
#     load_environment_variables()

#     # 🔹 **Step 2: Get Database Connection String**
#     DATABASE_URL = os.getenv("DATABASE_URL")

#     if not DATABASE_URL:
#         raise ValueError("ERROR: Missing DATABASE_URL environment variable!")


# # 🔹 **Step 3: Create the SQLAlchemy Engine**
# # def get_db_engine():
# #     """Create and return a database engine."""
# #     return create_engine(DATABASE_URL)


# # 🔹 **Step 4: Global MetaData Object (Prevents Duplication Issues)**
# metadata = MetaData()


# def get_metadata():
#     """Return a global SQLAlchemy MetaData object."""
#     return metadata


# # 🔹 **Step 5: Table Definitions**
# def define_game_plays_processor_test(metadata):
#     """Define the schema for game_plays_processor_test and return the table."""
#     table = Table(
#         "game_plays_processor_test",
#         metadata,
#         Column("play_id", String(20), primary_key=True),
#         Column("game_id", BigInteger),
#         Column("team_id_for", Integer, nullable=True),
#         Column("team_id_against", Integer, nullable=True),
#         Column("event", String(50)),
#         Column("secondaryType", String(50)),
#         Column("x", Float, nullable=True),
#         Column("y", Float, nullable=True),
#         Column("period", Integer),
#         Column("periodType", String(50)),
#         Column("periodTime", Integer),
#         Column("periodTimeRemaining", Integer),
#         Column("dateTime", DateTime(timezone=False)),
#         Column("goals_away", Integer, nullable=True),
#         Column("goals_home", Integer, nullable=True),
#         Column("description", String(255)),
#         Column("st_x", Integer, nullable=True),
#         Column("st_y", Integer, nullable=True),
#     )
#     return table  # ✅ Ensure it returns the table object


# def define_game_skater_stats_test(metadata):
#     """Define and return the schema for game_skater_stats_test."""
#     table = Table(
#         "game_skater_stats_test",
#         metadata,
#         Column("game_id", BigInteger),
#         Column("player_id", BigInteger),
#         Column("team_id", Integer),
#         Column("timeOnIce", Integer),
#         Column("assists", Integer),
#         Column("goals", Integer),
#         Column("shots", Integer),
#         Column("hits", Integer),
#         Column("powerPlayGoals", Integer),
#         Column("powerPlayAssists", Integer),
#         Column("penaltyMinutes", Integer),
#         Column("faceOffWins", Integer),
#         Column("faceoffTaken", Integer),
#         Column("takeaways", Integer),
#         Column("giveaways", Integer),
#         Column("shortHandedGoals", Integer),
#         Column("shortHandedAssists", Integer),
#         Column("blocked", Integer),
#         Column("plusMinus", Integer),
#         Column("evenTimeOnIce", Integer),
#         Column("shortHandedTimeOnIce", Integer),
#         Column("powerPlayTimeOnIce", Integer),
#     )
#     return table  # ✅ Ensure it returns the table object


# def create_table(engine, metadata, table):
#     """
#     Create a specific table in the database.

#     Parameters
#     ----------
#     engine : sqlalchemy.engine.Engine
#         The database engine.
#     metadata : sqlalchemy.MetaData
#         The SQLAlchemy MetaData instance.
#     table : sqlalchemy.Table
#         The table object to create.

#     """
#     metadata.create_all(engine, tables=[table])  # ✅ Create only the passed table
#     logging.info(f"Table '{table.name}' created or verified.")


# # 🔹 **Other Table Definitions**
# def define_game_plays_players_test(metadata):
#     """Define and return the schema for game_plays_players_test."""
#     return Table(
#         "game_plays_players_test",
#         metadata,
#         Column("play_id", String(20)),
#         Column("game_id", BigInteger, nullable=False),
#         Column("player_id", BigInteger, nullable=False),
#         Column("playerType", String(20)),
#     )


# def define_game_processor_test(metadata):
#     """Define and return the schema for game_processor_test."""
#     return Table(
#         "game_processor_test",
#         metadata,
#         Column("game_id", BigInteger, primary_key=True),
#         Column("season", Integer),
#         Column("type", String(20)),
#         Column("date_time_GMT", DateTime(timezone=False)),
#         Column("away_team_id", Integer),
#         Column("home_team_id", Integer),
#         Column("away_goals", Integer),
#         Column("home_goals", Integer),
#         Column("outcome", String(50)),
#         Column("home_rink_side_start", String(20)),
#         Column("venue", String(100)),
#         Column("venue_link", String(255)),
#         Column("venue_time_zone_id", String(50)),
#         Column("venue_time_zone_offset", Integer),
#         Column("venue_time_zone_tz", String(10)),
#     )
"""
db_utils.py.

Helper functions for database access and table creation.

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os
from urllib.parse import quote_plus

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


# 🔹 **Step 1: Load Environment Variables**
def load_environment_variables():
    """Load environment variables from `.env` file if not already set."""
    if not os.getenv("DATABASE_URL"):
        load_dotenv()
        logging.info("Environment variables loaded.")


# 🔹 **Step 2: Get Database Engine**
def get_db_engine():
    """
    Create and return a SQLAlchemy database engine.

    - Uses `DATABASE_URL` if available.
    - Otherwise, constructs a connection string from individual environment variables.

    Returns
    -------
        sqlalchemy.engine.Engine: A SQLAlchemy database engine instance.

    Raises
    ------
        ValueError: If `DATABASE_URL` is missing and required variables are not set.

    Environment Variables:
        - DATABASE_URL (optional, takes priority if set)
        - DATABASE_TYPE
        - DBAPI
        - ENDPOINT
        - USER
        - PASSWORD
        - PORT (default: 5432)
        - DATABASE

    """
    load_environment_variables()  # Ensure variables are loaded

    # Check if DATABASE_URL is set
    DATABASE_URL = os.getenv("DATABASE_URL")

    if DATABASE_URL:
        logging.info("Using DATABASE_URL from environment.")
        return create_engine(DATABASE_URL)

    # Otherwise, construct from individual variables
    DATABASE_TYPE = os.getenv("DATABASE_TYPE")
    DBAPI = os.getenv("DBAPI")
    ENDPOINT = os.getenv("ENDPOINT")
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    PORT = os.getenv("PORT", "5432")  # Default PostgreSQL port
    DATABASE = os.getenv("DATABASE")

    # Ensure all required variables are available
    missing_vars = [
        var
        for var in ["DATABASE_TYPE", "DBAPI", "ENDPOINT", "USER", "PASSWORD", "DATABASE"]
        if not os.getenv(var)
    ]
    if missing_vars:
        logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        raise ValueError("ERROR: One or more required environment variables are missing.")

    # URL encode the password in case it contains special characters
    encoded_password = quote_plus(PASSWORD)

    # Create the connection string
    connection_string = (
        f"{DATABASE_TYPE}+{DBAPI}://{USER}:{encoded_password}@{ENDPOINT}:{PORT}/{DATABASE}"
    )
    logging.info("Database connection string created.")

    return create_engine(connection_string)


# 🔹 **Step 3: Global MetaData Object (Prevents Duplication Issues)**
metadata = MetaData()


def get_metadata():
    """Return a global SQLAlchemy MetaData object."""
    return metadata


# 🔹 **Step 4: Table Definitions**
def define_game_skater_stats_test(metadata):
    """Define and return the schema for game_skater_stats_test."""
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


def define_game_plays_processor_test(metadata):
    """Define the schema for game_plays_processor_test and return the table."""
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
    """Define and return the schema for game_plays_players_test."""
    return Table(
        "game_plays_players_test",
        metadata,
        Column("play_id", String(20)),
        Column("game_id", BigInteger, nullable=False),
        Column("player_id", BigInteger, nullable=False),
        Column("playerType", String(20)),
    )


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
    logging.info(f"Table '{table.name}' created or verified.")
