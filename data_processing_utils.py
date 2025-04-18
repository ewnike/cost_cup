"""
Utility functions for data processing.

This module provides common functions for:
- Cleaning and processing data
- Inspecting and validating data
- Managing database operations

Eric Winiecke
February 17, 2025
"""

import logging
import os
import shutil
import string

import boto3
import botocore
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from db_utils import get_metadata

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Initialize S3 client
s3_client = boto3.client("s3")


def ensure_table_exists(engine, metadata, table_name, table_definition_function):
    """Ensure a table exists in the database, create it if necessary."""
    if table_name not in metadata.tables:
        logging.info(f"Creating missing table: {table_name}")
        table_definition_function(metadata)  # Dynamically define table
        metadata.create_all(engine)
        metadata.reflect(bind=engine)  # Refresh metadata


def clean_data(df, column_mapping, drop_duplicates=True):
    """
    Clean and format a DataFrame based on column mappings.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        column_mapping (dict): Dictionary mapping column names to expected types.
        drop_duplicates (bool): Whether to drop duplicates (default is True).

    Returns:
        pd.DataFrame: Cleaned DataFrame.

    """
    # Replace NaN values with 0 for integer and float columns
    for col in df.columns:
        if df[col].dtype.kind in "fi":  # Float or Integer
            df[col] = df[col].fillna(0)  # Convert NaN to 0
        else:
            df[col] = df[col].where(pd.notnull(df[col]), None)  # Keep None for strings

    # Convert columns to appropriate data types based on column_mapping
    for db_column, csv_column in column_mapping.items():
        if csv_column in df.columns:
            if db_column in ["x", "y"]:
                df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce").fillna(0)
            elif db_column == "dateTime":
                df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
            elif db_column in [
                "game_id",
                "player_id",
                "team_id_for",
                "team_id_against",
                "period",
                "timeOnIce",
                "assists",
                "goals",
                "shots",
                "hits",
                "powerPlayGoals",
                "powerPlayAssists",
                "penaltyMinutes",
                "faceOffWins",
                "faceoffTaken",
                "takeaways",
                "giveaways",
                "shortHandedGoals",
                "shortHandedAssists",
                "blocked",
                "plusMinus",
                "evenTimeOnIce",
                "shortHandedTimeOnIce",
                "powerPlayTimeOnIce",
            ]:
                df[csv_column] = pd.to_numeric(
                    df[csv_column], downcast="integer", errors="coerce"
                ).fillna(0)
            else:
                df[csv_column] = df[csv_column].astype(str).str.strip()

    # ✅ Drop duplicates only if drop_duplicates=True
    if drop_duplicates:
        df = df.drop_duplicates(ignore_index=True)

    return df


def convert_height(height_str):
    """Convert height from "6' 1"" format to total inches."""
    if pd.isnull(height_str):
        return None
    try:
        feet, inches = height_str.split("'")
        inches = inches.strip().replace('"', "")
        total_inches = int(feet) * 12 + int(inches)
        return total_inches
    except ValueError:
        return None


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

    # ✅ Log unique play_ids before processing
    logging.info(f"Before Suffix Addition - Unique play_ids: {df['play_id'].nunique()}")

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
            logging.debug(f"Updated play_id: {df.at[idx, 'play_id']}")  # Log updated play_id
        else:
            # Initialize the count for this play_id
            play_id_counts[play_id] = 1

    #     # ✅ Log unique play_ids after processing
    logging.info(f"After Suffix Addition - Unique play_ids: {df['play_id'].nunique()}")
    print(df.tail(100))
    return df


def clean_and_transform_data(df, column_mapping):
    """Apply specific transformations and further clean data."""
    # Start with general cleaning
    df = clean_data(df, column_mapping, drop_duplicates=False)

    # Check for the need to add suffixes to 'play_id' if the column exists
    if "play_id" in df.columns:
        df = add_suffix_to_duplicate_play_ids(df)

    # Apply specific transformations
    if "height" in df.columns:
        df["height"] = df["height"].apply(convert_height)  # Custom transformation example

    # Convert datetime fields
    if "birthDate" in df.columns:
        df["birthDate"] = pd.to_datetime(df["birthDate"])

    # Rename columns to match database schema
    df.rename(columns={"shootsCatches": "shootCatches"}, inplace=True)

    # Further sophisticated handling
    df = df.where(pd.notnull(df), None)  # Convert NaNs to None for database compatibility

    return df


def insert_data(df, table, session):
    """Insert DataFrame into a database table."""
    if df.shape[0] == 0:
        logging.error(f"DataFrame is empty! No data inserted into {table.name}.")
        return

    logging.info(f"Inserting {df.shape[0]} rows into {table.name}.")
    data = df.to_dict(orient="records")

    try:
        with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
            for record in data:
                session.execute(table.insert().values(**record))
                pbar.update(1)
        session.commit()
        logging.info(f"Data successfully inserted into {table.name}.")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error inserting data into {table.name}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected error inserting into {table.name}: {e}", exc_info=True)
    finally:
        session.close()


def clear_directory(directory):
    """Clear directory if it exists."""
    try:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            logging.info(f"Cleared directory: {directory}")
        os.makedirs(directory, exist_ok=True)
    except Exception as e:
        logging.error(f"Failed to clear directory {directory}: {e}")


def download_zip_from_s3(bucket_name, s3_file_key, local_download_path):
    """Download a ZIP file from S3 and save it as a file, ensuring correct behavior."""
    if not local_download_path:  # Check if the download path is empty
        logging.error("Download path is empty. Skipping download operation.")
        return

    # Ensuring the directory exists
    directory = os.path.dirname(local_download_path)
    if directory:  # Only attempt to create the directory if it's not empty
        os.makedirs(directory, exist_ok=True)
    else:
        logging.error("Derived directory path is empty. Cannot ensure directory existence.")
        return

    # Proceed with the download if the path checks out
    try:
        s3_client.download_file(bucket_name, s3_file_key, local_download_path)
        logging.info(f"Successfully downloaded {s3_file_key} to {local_download_path}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logging.error(f"File not found: {s3_file_key} in bucket {bucket_name}.")
        elif e.response["Error"]["Code"] == "403":
            logging.error(
                f"Access denied to {s3_file_key} in bucket {bucket_name}. Check permissions."
            )
        else:
            logging.error(f"Error downloading file from S3: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to download file from S3: {e}")
        raise


def extract_zip(zip_path, extract_to):
    """Extract a ZIP file to the specified directory, ensuring correct file structure."""
    # Check if the zip_path is provided and it exists
    if not zip_path or not os.path.exists(zip_path):
        if not zip_path:
            logging.info("No ZIP path provided; skipping extraction.")
        else:
            logging.error(f"ERROR: ZIP file not found: {zip_path}")
        return []

    # Ensure the extraction directory exists
    os.makedirs(extract_to, exist_ok=True)

    try:
        # Extract the ZIP file into `data/extracted/`
        shutil.unpack_archive(zip_path, extract_to)
        logging.info(f"Extracted {zip_path} to {extract_to}")

        # Return list of extracted files
        return os.listdir(extract_to)
    except Exception as e:
        logging.error(f"ERROR: Failed to extract {zip_path} - {e}")
        logging.info(f"{extract_to}")
        logging.info(f"{zip_path}")
        return []


def process_and_insert_data(config):
    """
    Download, extract, clean, and insert data into a database table.

    Args:
    ----
        config (dict): Dictionary containing configuration for the operation. Expected keys include:
            - bucket_name (str): The S3 bucket name.
            - s3_file_key (str): The file key in S3.
            - local_zip_path (str): The path to save the downloaded zip file.
            - local_extract_path (str): The directory to extract files into.
            - local_download_path (str): The path to download the file from S3.
            - expected_csv_filename (str): The expected CSV file name inside the extracted files.
            - table_definition_function (function): Function to define the table schema.
            - table_name (str): The target database table name.
            - column_mapping (dict): Column mapping for data cleaning.
            - engine (sqlalchemy.engine.Engine): The database engine instance.
            - handle_zip (bool): Flag indicating whether the file is a zip archive.

    """
    session_factory = sessionmaker(bind=config["engine"])
    session = session_factory()

    # Clear old data
    clear_directory(config["local_extract_path"])

    # Define the correct download path based on whether handling a ZIP
    download_path = (
        config["local_zip_path"]
        if config["handle_zip"]
        else os.path.join(config["local_download_path"], config["expected_csv_filename"])
    )

    download_zip_from_s3(config["bucket_name"], config["s3_file_key"], download_path)

    if config["handle_zip"]:
        extract_zip(download_path, config["local_extract_path"])
        csv_file_path = os.path.join(config["local_extract_path"], config["expected_csv_filename"])
        if not os.path.exists(csv_file_path):
            logging.error(f"Extracted file not found after extraction: {csv_file_path}")
            return
        clear_directory(config["local_download_path"])
    else:
        csv_file_path = download_path
        if not os.path.exists(csv_file_path):
            logging.error(f"Downloaded file not found at path: {csv_file_path}")
            return

    try:
        if csv_file_path.endswith(".csv") or csv_file_path.endswith(".csv.xls"):
            df = pd.read_csv(csv_file_path)
        else:
            df = pd.read_excel(csv_file_path, engine="openpyxl")
    except Exception as e:
        logging.error(f"Error reading file {csv_file_path}: {e}")
        return

    df = clean_and_transform_data(df, config["column_mapping"])

    ensure_table_exists(
        config["engine"],
        get_metadata(),
        config["table_name"],
        config["table_definition_function"],
    )

    try:
        logging.info(f"Inserting data into table: {config['table_name']}")
        insert_data(df, get_metadata().tables[config["table_name"]], session)
        logging.info(f"Data successfully inserted into {config['table_name']}.")
    except Exception as e:
        session.rollback()
        logging.error(f"Error inserting data into {config['table_name']}: {e}")
    finally:
        session.close()
