"""
October 1, 2024
Helper function for accessing
the database and reducing clutter from
redundant code.
"""

import logging
import os

from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine


# Load environment variables from the .env file
def load_environment_variables():
    load_dotenv()
    logging.info("Environment variables loaded.")  # Debug log to confirm loading


def get_db_engine():
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
    return MetaData()
