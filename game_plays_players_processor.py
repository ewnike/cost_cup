"""
October 24, 2024
writing code to upload game_plays_player
file from aws s3 bucket, clean data, and
insert data into the hockey_stats data table
"""

import logging
import os
import shutil
import string

import pandas as pd
from sqlalchemy import BigInteger, Column, String, Table
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from db_utils import get_db_engine, get_metadata
from S3_Utils import download_from_s3

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


game_plays_players = Table(
    "game_plays_players",
    metadata,
    Column("play_id", String(20)),
    Column("game_id", BigInteger, nullable=False),
    Column("player_id", BigInteger, nullable=False),
    Column("playerType", String(20)),
)

# Create the table in the database if it doesn’t exist
metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Define column mapping with data types
column_mapping = {
    "play_id": "str",
    "game_id": "int64",
    "player_id": "int64",
    "playerType": "str",
}


def clean_data(df, column_mapping):
    """Clean and format the DataFrame according to column_mapping."""
    # Replace NaN with None for compatibility with SQLAlchemy and PostgreSQL insertions
    df = df.where(pd.notnull(df), None)

    # Apply data type conversions without renaming columns
    for column, dtype in column_mapping.items():
        if column in df.columns:
            if dtype == "int64":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(
                    "Int64"
                )  # Null-safe integer type
            elif dtype == "str":
                df[column] = df[column].astype(str)
            else:
                df[column] = df[column].astype(dtype, errors="ignore")

    # Clean object-type columns, truncating strings and removing whitespace
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].apply(
            lambda x: str(x).strip()[:255] if isinstance(x, str) else x
        )

    return df


# S3 configuration and paths
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = os.getenv("S3_FILE_KEY", "game_plays_players.csv.zip")
local_zip_path = os.path.join("data/download", "game_plays_players.zip")

# Local paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game_plays_players.zip")
data_path = os.getenv("DATA_PATH", "data")  # Path to the data folder


# def download_zip_from_s3(bucket, key, download_path):
#     """Function to download a zip file from S3"""
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


def extract_zip(zip_path, extract_to):
    """Function to extract zip files"""
    shutil.unpack_archive(zip_path, extract_to)
    logging.info(f"Extracted {zip_path} to {extract_to}")
    return os.listdir(extract_to)


def clear_directory(directory):
    """Function to clear a directory"""
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Cleared directory: {directory}")
    os.makedirs(directory, exist_ok=True)


def process_and_clean_data(file_path, column_mapping):
    """Loads, verifies, and cleans the CSV data."""
    # Load data with all columns as strings for flexibility
    df = pd.read_csv(file_path, dtype=str)
    logging.info(f"Columns in loaded CSV: {list(df.columns)}")
    print("Columns in loaded CSV:", list(df.columns))  # Debugging output

    # Standardize column names to lowercase and log the result
    df.columns = df.columns.str.strip().str.lower()
    logging.info(f"Standardized columns in DataFrame: {list(df.columns)}")
    print("Standardized columns in DataFrame:", list(df.columns))  # Debugging output

    # Rename `playertype` to `player_type` for consistency, if applicable
    if "playertype" in df.columns:
        df = df.rename(columns={"playerType": "player_type"})

    # Update column_mapping keys to match standardized format
    standardized_mapping = {k.lower(): v for k, v in column_mapping.items()}

    # Check for missing columns based on standardized column_mapping keys
    expected_columns = set(standardized_mapping.keys())
    missing_cols = expected_columns - set(df.columns)
    if missing_cols:
        raise KeyError(f"Missing expected columns: {missing_cols}")

    # Clean the data using the specified column mappings
    df = clean_data(df, standardized_mapping)
    print("DataFrame after clean_data:", df.head())  # Debugging output
    df = add_suffix_to_duplicate_play_ids(df)  # Ensure unique play_ids
    return df


def add_suffix_to_duplicate_play_ids(df):
    """Add alphabetical suffixes to duplicate 'play_id' values to ensure uniqueness."""
    # Debugging output for column names
    print(
        "Columns in DataFrame when entering add_suffix_to_duplicate_play_ids:",
        list(df.columns),
    )

    # Verify the existence of 'play_id' column before proceeding
    if "play_id" not in df.columns:
        raise KeyError("The 'play_id' column is missing in the DataFrame!")

    play_id_counts = {}  # Dictionary to track occurrences of each play_id

    # Iterate over the DataFrame index to avoid issues with Series indexing
    for idx in df.index:
        play_id = df.at[idx, "play_id"]  # Access play_id directly by index
        logging.debug(f"Processing play_id: {play_id}")  # Debug log for each play_id

        # Check if play_id has already been seen
        if play_id in play_id_counts:
            # Increment count and add the appropriate suffix
            play_id_counts[play_id] += 1
            suffix = string.ascii_lowercase[play_id_counts[play_id] - 1]
            df.at[idx, "play_id"] = f"{play_id}{suffix}"
            logging.debug(
                f"Updated play_id: {df.at[idx, 'play_id']}"
            )  # Log updated play_id
        else:
            # Initialize the count for this play_id
            play_id_counts[play_id] = 1

    return df


def create_table(engine):
    """Function to create the table if it does not exist"""
    metadata.create_all(engine)


def clear_table(engine, table):
    """Function to clear the table if it exists"""
    with engine.connect() as connection:
        connection.execute(table.delete())
        connection.commit()
        logging.info(f"Table {table.name} cleared.")


def insert_data(df, table):
    """Function to insert data into the database"""
    data = df.to_dict(orient="records")
    session = Session()  # Properly instantiate a session
    try:
        with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
            for record in data:
                try:
                    session.execute(table.insert().values(**record))
                    session.commit()  # Commit after each insert
                    pbar.update(1)
                    logging.info(f"Inserted record: {record}")
                except IntegrityError as e:
                    session.rollback()  # Rollback only on conflict
                    logging.warning(f"Duplicate entry, skipping record: {record} - {e}")
                except SQLAlchemyError as e:
                    session.rollback()  # Rollback on other errors
                    logging.error(f"SQLAlchemy error inserting record {record}: {e}")
                    break  # Optionally stop on error
        logging.info(f"All data inserted into {table.name}")
    except Exception as e:
        logging.error(f"General error during insertion into {table.name}: {e}")
    finally:
        session.close()  # Ensure session is closed


def process_and_insert_csv(csv_file_path, table, column_mapping):
    """Function to process and insert data from a CSV file"""
    try:
        logging.info(f"Processing {csv_file_path} for table {table.name}")
        # Load CSV data
        df = pd.read_csv(csv_file_path)
        logging.info(f"DataFrame for {table.name} loaded with {len(df)} records")

        # Log datatypes and a data sample for debugging
        logging.info("Column Datatypes:")
        logging.info(df.dtypes)
        logging.info("Sample data before cleaning:")
        logging.info(df.head())

        # Clean the data based on column mapping
        df = clean_data(df, column_mapping)
        logging.info(f"DataFrame for {table.name} cleaned with {len(df)} records")

        # Log a sample of cleaned data for further debugging
        logging.info("Sample data after cleaning:")
        logging.info(df.head())

        # Add suffixes to duplicate play_ids to ensure uniqueness
        df = add_suffix_to_duplicate_play_ids(df)
        logging.info("Duplicate play_ids have been made unique.")
        logging.info("DataFrame after adding suffixes:")
        logging.info(df.head())  # Log data sample after suffix addition

        # Clear the table if it exists
        clear_table(engine, table)

        # Insert each row from cleaned DataFrame into the table
        insert_data(df, table)

    except FileNotFoundError as e:
        logging.error(f"File not found: {csv_file_path} - {e}")
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


def test_db_connection():
    """Test if the database connection is successful."""
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            logging.info("Database connection test successful.")
            print("Database connection test successful.")  # Console confirmation
    except Exception as e:
        logging.error(f"Database connection test failed: {e}")
        print(f"Database connection test failed: {e}")  # Console output for errors


def main():
    """Main function to handle downloading, extracting, cleaning, and inserting data."""
    # Test database connection
    test_db_connection()

    # Ensure the table exists in the database
    create_table(engine)

    # Clear the extraction directory
    clear_directory(local_extract_path)

    # Download the zip file from S3
    download_from_s3(bucket_name, s3_file_key, local_zip_path)

    # Check if the zip file was successfully downloaded
    if os.path.exists(local_zip_path):
        # Extract the zip file
        extracted_files = extract_zip(local_zip_path, local_extract_path)
        csv_file_path = os.path.join(local_extract_path, "game_plays_players.csv")

        # Check if the CSV file exists
        if os.path.exists(csv_file_path):
            # Process, clean, and insert data directly
            process_and_insert_csv(csv_file_path, game_plays_players, column_mapping)

        else:
            logging.error(f"CSV file {csv_file_path} not found.")
    else:
        logging.error(f"Zip file {local_zip_path} not found.")

    # Clear the data and extracted folders after processing
    clear_directory(data_path)
    clear_directory(local_extract_path)


if __name__ == "__main__":
    main()
