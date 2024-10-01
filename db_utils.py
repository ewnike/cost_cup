# db_utils.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData

# Function to load environment variables and create a database connection
def get_db_engine():
    # Load environment variables from the .env file
    load_dotenv()

    # Retrieve database connection parameters from environment variables
    DATABASE_TYPE = os.getenv("DATABASE_TYPE")
    DBAPI = os.getenv("DBAPI")
    ENDPOINT = os.getenv("ENDPOINT")
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    PORT = int(os.getenv("PORT", 5432))  # Provide default value if not set
    DATABASE = os.getenv("DATABASE")

    # Create the connection string
    connection_string = (
        f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
    )

    # Return the SQLAlchemy engine
    return create_engine(connection_string)

# Function to return SQLAlchemy metadata
def get_metadata():
    return MetaData()
