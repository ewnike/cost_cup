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
from sqlalchemy import MetaData, create_engine


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
    DATABASE_TYPE = os.getenv("DATABASE_TYPE")
    DBAPI = os.getenv("DBAPI")
    ENDPOINT = os.getenv("ENDPOINT")
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    PORT = os.getenv("PORT", 5432)
    DATABASE = os.getenv("DATABASE")

    # Check if required environment variables are loaded
    if not all([DATABASE_TYPE, DBAPI, ENDPOINT, USER, PASSWORD, DATABASE]):
        logging.error("Missing one or more required environment variables.")
        raise ValueError("Missing one or more required environment variables.")

    # Create the connection string
    connection_string = (
        f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
    )
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
