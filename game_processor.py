# """
# August 11, 2024.

# Code to load game data from AWS S3 buscket
# and to read nad insert the data into a
# defined table in hockey_stats db.

# Eric Winiecke.
# """

# import logging
# import os
# import shutil

# import boto3
# import botocore
# import pandas as pd
# from dotenv import load_dotenv
# from sqlalchemy import (
#     BigInteger,
#     Column,
#     DateTime,
#     Integer,
#     MetaData,
#     String,
#     Table,
#     create_engine,
# )
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.orm import sessionmaker
# from tqdm import tqdm

# # Configure logging
# logging.basicConfig(
#     filename="data_processing.log",
#     level=logging.INFO,
#     format="%(asctime)s:%(levelname)s:%(message)s",
# )

# # Load environment variables from .env file
# load_dotenv()

# # Database connection parameters
# DATABASE_TYPE = os.getenv("DATABASE_TYPE")
# DBAPI = os.getenv("DBAPI")
# ENDPOINT = os.getenv("ENDPOINT")
# USER = os.getenv("USER")
# PASSWORD = os.getenv("PASSWORD")
# PORT = int(os.getenv("PORT", 5432))
# DATABASE = os.getenv("DATABASE", "hockey_stats")

# # Create the connection string
# connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

# # Define table schemas
# metadata = MetaData()

# game = Table(
#     "game",
#     metadata,
#     Column("game_id", BigInteger, primary_key=True),
#     Column("season", Integer),
#     Column("type", String(50)),
#     Column("date_time_GMT", DateTime(timezone=True)),
#     Column("away_team_id", Integer),
#     Column("home_team_id", Integer),
#     Column("away_goals", Integer),
#     Column("home_goals", Integer),
#     Column("outcome", String(100)),
#     Column("home_rink_side_start", String(100)),
#     Column("venue", String(100)),
#     Column("venue_link", String(100)),
#     Column("venue_time_zone_id", String(50)),
#     Column("venue_time_zone_offset", Integer),
#     Column("venue_time_zone_tz", String(25)),
# )

# # Create the database engine
# engine = create_engine(connection_string)
# Session = sessionmaker(bind=engine)


# def clean_data(df, column_mapping):
#     """Function to clean the DataFrame."""  # noqa: D401
#     # Replace NaN with None for all columns
#     df = df.where(pd.notnull(df), None)

#     # Truncate long strings and remove whitespace
#     for column, dtype in df.dtypes.items():
#         if dtype == "object":
#             df[column] = df[column].apply(
#                 lambda x: str(x).strip()[:255] if isinstance(x, str) else x
#             )

#     # Convert columns to appropriate data types based on column_mapping
#     for csv_column in column_mapping.items():
#         if "int" in str(df[csv_column].dtype):
#             df[csv_column] = pd.to_numeric(df[csv_column], downcast="integer", errors="coerce")
#         elif "float" in str(df[csv_column].dtype):
#             df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce")
#         elif "date_time" in csv_column:
#             df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")

#     # Remove duplicates
#     df = df.drop_duplicates(ignore_index=True)

#     return df


# def create_table(engine):
#     """Function to create the table if it does not exist."""  # noqa: D401
#     metadata.create_all(engine)


# def clear_table(engine, table):
#     """Function to clear the table if it exists."""  # noqa: D401
#     with engine.connect() as connection:
#         connection.execute(table.delete())
#         connection.commit()
#         logging.info(f"Table {table.name} cleared.")


# def insert_data(df, table):
#     """Function to insert data into the database."""  # noqa: D401
#     data = df.to_dict(orient="records")
#     with Session() as session:
#         try:
#             with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
#                 for record in data:
#                     session.execute(table.insert().values(**record))
#                     session.commit()
#                     pbar.update(1)
#             logging.info(f"Data inserted successfully into {table.name}")
#         except SQLAlchemyError as e:
#             session.rollback()
#             logging.error(f"Error inserting data into {table.name}: {e}")


# def inspect_data(df):
#     """Function to inspect data for errors."""  # noqa: D401
#     # Check unique values in critical columns
#     logging.info(f"Unique values in 'game_id': {df['game_id'].unique()}")
#     logging.info(f"Unique values in 'away_team_id': {df['away_team_id'].unique()}")
#     logging.info(f"Unique values in 'home_team_id': {df['home_team_id'].unique()}")

#     # Convert columns to numeric and identify problematic rows
#     for column in ["game_id", "away_team_id", "home_team_id"]:
#         df[column] = pd.to_numeric(df[column], errors="coerce")
#         logging.warning(f"Rows with large values in {column}:")
#         logging.warning(df[df[column] > 1000])
#         logging.warning(f"Rows with negative values in {column}:")
#         logging.warning(df[df[column] < 0])


# def process_and_insert_csv(csv_file_path, table, column_mapping):
#     """Function to process and insert data from a CSV file."""  # noqa: D401
#     try:
#         logging.info(f"Processing {csv_file_path} for table {table.name}")
#         df = pd.read_csv(csv_file_path)
#         logging.info(f"DataFrame for {table.name} loaded with {len(df)} records")

#         # Print the datatype of each column
#         logging.info("Column Datatypes:")
#         logging.info(df.dtypes)

#         # Print a sample of the data to debug
#         logging.info("Sample data:")
#         logging.info(df.head())

#         # Inspect data for potential errors
#         inspect_data(df)

#         # Clean the data
#         df = clean_data(df, column_mapping)
#         logging.info(f"DataFrame for {table.name} cleaned with {len(df)} records")

#         # Print a sample of the cleaned data to debug
#         logging.info("Cleaned sample data:")
#         logging.info(df.head())

#         # Clear the table if it exists
#         clear_table(engine, table)

#         # Insert the cleaned data into the database
#         insert_data(df, table)

#     except FileNotFoundError as e:
#         logging.error(f"File not found: {csv_file_path} - {e}")


# # Define the column mapping for game.csv
# column_mapping = {
#     "game_id": "game_id",
#     "season": "season",
#     "type": "type",
#     "date_time_GMT": "date_time_GMT",
#     "away_team_id": "away_team_id",
#     "home_team_id": "home_team_id",
#     "away_goals": "away_goals",
#     "home_goals": "home_goals",
#     "outcome": "outcome",
#     "home_rink_side_start": "home_rink_side_start",
#     "venue": "venue",
#     "venue_link": "venue_link",
#     "venue_time_zone_id": "venue_time_zone_id",
#     "venue_time_zone_offset": "venue_time_zone_offset",
#     "venue_time_zone_tz": "venue_time_zone_tz",
# }

# # S3 client
# s3_client = boto3.client("s3")
# bucket_name = os.getenv("S3_BUCKET_NAME")
# s3_file_key = os.getenv("S3_FILE_KEY", "game.csv.zip")

# # Local paths
# local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
# local_zip_path = os.path.join(local_extract_path, "game.zip")
# data_path = os.getenv("DATA_PATH", "data")  # Path to the data folder


# def download_zip_from_s3(bucket, key, download_path):
#     """Function to download a zip file from S3."""  # noqa: D401
#     logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
#     try:
#         s3_client.download_file(bucket, key, download_path)
#         logging.info(f"Downloaded {key} from S3 to {download_path}")
#     except botocore.exceptions.ClientError as e:
#         logging.error(f"Error: {e}")
#         if e.response["Error"]["Code"] == "404":
#             logging.error("The object does not exist.")
#         else:
#             raise


# def extract_zip(zip_path, extract_to):
#     """Function to extract zip files."""  # noqa: D401
#     shutil.unpack_archive(zip_path, extract_to)
#     logging.info(f"Extracted {zip_path} to {extract_to}")
#     return os.listdir(extract_to)


# def clear_directory(directory):
#     """Function to clear a directory."""  # noqa: D401
#     if os.path.exists(directory):
#         shutil.rmtree(directory)
#         logging.info(f"Cleared directory: {directory}")
#     os.makedirs(directory, exist_ok=True)


# def main():
#     """Main function to handle the workflow."""  # noqa: D401
#     # Clear the extracted folder and recreate it
#     clear_directory(local_extract_path)

#     # Download and extract the zip file from S3
#     download_zip_from_s3(bucket_name, s3_file_key, local_zip_path)
#     extracted_files = extract_zip(local_zip_path, local_extract_path)
#     logging.info(f"Extracted files: {extracted_files}")

#     # Path to game.csv file
#     game_csv_file_path = os.path.join(local_extract_path, "game.csv")

#     # Create the table if it does not exist
#     create_table(engine)

#     # Process and insert game.csv
#     if os.path.exists(game_csv_file_path):
#         process_and_insert_csv(game_csv_file_path, game, column_mapping)
#     else:
#         logging.error(f"CSV file {game_csv_file_path} not found")

#     # Clear the data and extracted folders after processing
#     clear_directory(data_path)
#     clear_directory(local_extract_path)


# if __name__ == "__main__":
#     main()
# import logging
# import os
# import shutil

# import boto3
# import botocore
# import pandas as pd
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.orm import sessionmaker
# from tqdm import tqdm

# from data_processing_utils import (
#     clean_data,
#     clear_directory,
#     extract_zip,
#     insert_data,
#     process_and_insert_csv,
# )
# from db_utils import create_table, get_db_engine, get_metadata
# from s3_utils import download_from_s3

# # Configure logging
# logging.basicConfig(
#     filename="data_processing.log",
#     level=logging.INFO,
#     format="%(asctime)s:%(levelname)s:%(message)s",
# )

# # Initialize database connection
# engine = get_db_engine()
# metadata = get_metadata()
# Session = sessionmaker(bind=engine)

# # Define table schema for game
# game = metadata.tables.get("game")

# # Define column mapping for CSV to database table
# game_column_mapping = {
#     "game_id": "game_id",
#     "season": "season",
#     "type": "type",
#     "date_time_GMT": "date_time_GMT",
#     "away_team_id": "away_team_id",
#     "home_team_id": "home_team_id",
#     "away_goals": "away_goals",
#     "home_goals": "home_goals",
#     "outcome": "outcome",
#     "home_rink_side_start": "home_rink_side_start",
#     "venue": "venue",
#     "venue_link": "venue_link",
#     "venue_time_zone_id": "venue_time_zone_id",
#     "venue_time_zone_offset": "venue_time_zone_offset",
#     "venue_time_zone_tz": "venue_time_zone_tz",
# }

# # Load environment variables
# bucket_name = os.getenv("S3_BUCKET_NAME")
# s3_file_key = os.getenv("S3_FILE_KEY", "game.csv.zip")
# local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
# local_zip_path = os.path.join(local_extract_path, "game.zip")
# data_path = os.getenv("DATA_PATH", "data")


# def main():
#     """Execute the workflow for downloading, extracting, and processing data."""
#     try:
#         # Prepare directories
#         clear_directory(local_extract_path)

#         # Download and extract data
#         download_from_s3(bucket_name, s3_file_key, local_zip_path)
#         extracted_files = extract_zip(local_zip_path, local_extract_path)
#         logging.info(f"Extracted files: {extracted_files}")

#         # Process CSV file
#         game_csv_file_path = os.path.join(local_extract_path, "game.csv")
#         create_table(engine, metadata)

#         # STOP EXECUTION BEFORE INSERTING INTO DATABASE
#         if os.path.exists(game_csv_file_path):
#             logging.info("Skipping data insertion step.")  # Add a log entry
#             return  # Early exit to prevent inserting data into the database

#             # process_and_insert_csv(
#             #     game_csv_file_path,
#             #     game,
#             #     game_column_mapping,
#             #     engine,
#             # )

#         else:
#             logging.error(f"CSV file {game_csv_file_path} not found")

#         # Cleanup directories
#         clear_directory(data_path)
#         clear_directory(local_extract_path)

#     except Exception as e:
#         logging.error(f"Error in main execution: {e}")
#         raise


# if __name__ == "__main__":
#     main()
"""
game_processor.py.

This script downloads, extracts, cleans, and inserts `game` data
from AWS S3 into a PostgreSQL test table.

Refactored to use:
- `s3_utils.py` for S3 operations.
- `data_processing_utils.py` for data cleaning, extraction, and database handling.
- `db_utils.py` for database connection management.

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os

import pandas as pd
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker

from data_processing_utils import (
    clean_data,
    clear_directory,
    ensure_table_exists,
    extract_zip,
    insert_data,
)
from db_utils import get_db_engine, get_metadata
from s3_utils import download_from_s3

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()
Session = sessionmaker(bind=engine)

# Define test table name
TABLE_NAME = "game_processor_test"

# AWS S3 Config
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = "game.csv.zip"

# Local Paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")
local_zip_path = os.path.join(local_extract_path, "game.zip")
csv_file_path = os.path.join(local_extract_path, "game.csv")

# Define column mapping
column_mapping = {
    "game_id": "int64",
    "season": "int64",
    "type": "str",
    "date_time_GMT": "datetime64",
    "away_team_id": "int64",
    "home_team_id": "int64",
    "away_goals": "int64",
    "home_goals": "int64",
    "outcome": "str",
    "home_rink_side_start": "str",
    "venue": "str",
    "venue_link": "str",
    "venue_time_zone_id": "str",
    "venue_time_zone_offset": "int64",
    "venue_time_zone_tz": "str",
}


def process_and_clean_data(file_path, column_mapping):
    """Load, verify, and apply game-specific cleaning to the CSV data."""
    # Step 1: Read CSV into DataFrame
    df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

    # Log raw data before cleaning
    logging.info(f"Raw Data Sample Before Cleaning:\n{df.head()}")
    logging.info(f"CSV Columns Before Processing: {list(df.columns)}")

    # Step 2: Apply Generic Cleaning from data_processing_utils
    df = clean_data(df, column_mapping)

    # Truncate long strings and remove whitespace
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].apply(
            lambda x: str(x).strip()[:255] if isinstance(x, str) else x
        )

    # Drop rows where 'game_id' or 'season' is missing (specific to game table)
    initial_row_count = len(df)
    df = df.dropna(subset=["game_id", "season"])
    logging.info(
        f"Dropped {initial_row_count - len(df)} rows with missing 'game_id' or 'season'."
    )

    # Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    # Log cleaned data
    logging.info(f"Processed Data Sample:\n{df.head()}")
    logging.info(f"Processed Data Shape: {df.shape}")

    return df


def process_and_insert_data():
    """Execute downloading, extracting, cleaning, and inserting data into the test table."""
    session = Session()

    # Step 1: Clear extraction directory
    clear_directory(local_extract_path)

    # Step 2: Download ZIP from S3
    download_from_s3(bucket_name, s3_file_key, local_zip_path)

    # Step 3: Extract ZIP file
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    if "game.csv" not in extracted_files:
        logging.error(f"Missing expected file: {csv_file_path}")
        return

    # Step 4: Ensure the test table exists
    ensure_table_exists(engine, metadata, TABLE_NAME)

    # Step 5: Fetch table reference
    game_test = Table(TABLE_NAME, metadata, autoload_with=engine)

    # Step 6: Process and clean data
    df = process_and_clean_data(csv_file_path, column_mapping)

    # Step 7: Insert data into the table
    try:
        logging.info(f"Inserting data into table: {TABLE_NAME}")
        insert_data(df, game_test, session)
        logging.info(f"Data successfully inserted into {TABLE_NAME}.")
    except Exception as e:
        logging.error(f"Error inserting data into {TABLE_NAME}: {e}", exc_info=True)

    session.close()
    logging.info("Processing completed successfully.")


if __name__ == "__main__":
    process_and_insert_data()
