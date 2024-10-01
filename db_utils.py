import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData

# Load environment variables from the .env file
def load_environment_variables():
    # Load environment variables from the .env file
    load_dotenv()
    print("Environment variables loaded!")  # Debug message to verify loading

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
        raise ValueError("Missing one or more required environment variables.")

    # Create the connection string
    connection_string = (
        f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
    )
    print("Connection string:", connection_string)  # Debugging print

    # Return the SQLAlchemy engine
    return create_engine(connection_string)

# Function to return SQLAlchemy metadata
def get_metadata():
    return MetaData()



