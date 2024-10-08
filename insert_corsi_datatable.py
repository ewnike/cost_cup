"""
August 11, 2024
Code for inserting newly created
corsi data into defined tables in the
hockey_stats database.
Eric Winiecke
"""

import os

import pandas as pd
#from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Integer,
    MetaData,
    Table,
    #create_engine,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()
# # Load environment variables from the .env file
# load_dotenv()

# # Retrieve database connection parameters from environment variables
# DATABASE_TYPE = os.getenv("DATABASE_TYPE")
# DBAPI = os.getenv("DBAPI")
# ENDPOINT = os.getenv("ENDPOINT")
# USER = os.getenv("USER")
# PASSWORD = os.getenv("PASSWORD")
# PORT = int(os.getenv("PORT", 5432))  # Provide default value if not set
# DATABASE = os.getenv("DATABASE")

# # Create the connection string
# connection_string = (
#     f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
# )
# engine = create_engine(connection_string)

# Define metadata and tables
metadata = MetaData()


def create_corsi_table(table_name):
    """Define table creation function to avoid repetition"""
    return Table(
        table_name,
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("team_id", Integer),
        Column("corsi_for", Float, nullable=True),
        Column("corsi_against", Float, nullable=True),
        Column("corsi", Float, nullable=True),
        Column("CF_Percent", Float, nullable=True),
    )


# Create tables for each season
seasons = ["20152016", "20162017", "20172018"]
tables = {season: create_corsi_table(f"raw_corsi_{season}") for season in seasons}

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)


def insert_data_from_csv(engine, table_name, file_path):
    """insert data"""
    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"Data inserted successfully into {table_name}")

        # Remove the file after successful insertion
        os.remove(file_path)
        print(f"File {file_path} deleted successfully.")

    except SQLAlchemyError as e:
        print(f"Error inserting data into {table_name}: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {file_path} - {e}")
    except Exception as e:
        print(f"Error occurred while processing file '{file_path}': {e}")


# Define directories and mappings
csv_files_and_mappings = [
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/corsi_stats/corsi_20152016.csv",
        "raw_corsi_20152016",
    ),
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/corsi_stats/corsi_20162017.csv",
        "raw_corsi_20162017",
    ),
    (
        "/Users/ericwiniecke/Documents/github/cost_cup/corsi_stats/corsi_20172018.csv",
        "raw_corsi_20172018",
    ),
]

with Session() as session:
    for file_path, table_name in csv_files_and_mappings:
        insert_data_from_csv(engine, table_name, file_path)

    print("Data inserted successfully into all tables")
