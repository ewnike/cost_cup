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
    """Clear a directory and recreate it."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Cleared directory: {directory}")
    os.makedirs(directory, exist_ok=True)


def download_zip_from_s3(bucket, key, download_path):
    """Download a ZIP file from S3 and save it as a file, ensuring correct behavior."""
    logging.info(f"Downloading {key} from S3 bucket {bucket} to {download_path}")

    # ✅ Ensure the download directory exists
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    try:
        # ✅ If download_path exists as a directory, remove it
        if os.path.exists(download_path) and os.path.isdir(download_path):
            shutil.rmtree(download_path)

        # ✅ Download the file from S3
        s3_client.download_file(bucket, key, download_path)

        # ✅ Verify the file is correctly downloaded
        if not os.path.isfile(download_path):
            raise FileNotFoundError(f"S3 download failed: {download_path} is not a file.")

        logging.info(f"Successfully downloaded {key} from S3 to {download_path}")

    except botocore.exceptions.ClientError as e:
        logging.error(f"Error downloading file from S3: {e}")
        if e.response["Error"]["Code"] == "404":
            logging.error(f"ERROR: The object {key} does not exist in bucket {bucket}.")
        else:
            raise


def extract_zip(zip_path, extract_to):
    """Extract a ZIP file to the specified directory, ensuring correct file structure."""
    if not os.path.exists(zip_path):
        logging.error(f"ERROR: ZIP file not found: {zip_path}")
        return []

    # ✅ Ensure the extraction directory exists
    os.makedirs(extract_to, exist_ok=True)

    try:
        # ✅ Extract the ZIP file into `data/extracted/`
        shutil.unpack_archive(zip_path, extract_to)
        logging.info(f"Extracted {zip_path} to {extract_to}")

        # ✅ Return list of extracted files
        return os.listdir(extract_to)
    except Exception as e:
        logging.error(f"ERROR: Failed to extract {zip_path} - {e}")
        return []


def process_and_insert_data(
    bucket_name,
    s3_file_key,
    local_zip_path,
    local_extract_path,  # ✅ New parameter for extraction
    expected_csv_filename,
    table_definition_function,
    table_name,
    column_mapping,
    engine,
):
    """
    Download, extract, clean, and insert data into a database table.

    Args:
    ----
        bucket_name (str): The S3 bucket name.
        s3_file_key (str): The file key in S3.
        local_zip_path (str): The path to save the downloaded zip file.
        local_extract_path (str): The directory to extract files into. ✅
        expected_csv_filename (str): The expected CSV file name inside the extracted files.
        table_definition_function (function): Function to define the table schema.
        table_name (str): The target database table name.
        column_mapping (dict): Column mapping for data cleaning.
        engine (sqlalchemy.engine.Engine): The database engine instance.

    """
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # **Step 1: Clear Old Data**
    clear_directory(local_extract_path)  # ✅ Extracted files go here

    # **Step 2: Download and Extract Data**
    download_zip_from_s3(bucket_name, s3_file_key, local_zip_path)
    extracted_files = extract_zip(
        local_zip_path, local_extract_path
    )  # ✅ Extract to `local_extract_path`
    logging.info(f"Extracted files: {extracted_files}")

    csv_file_path = os.path.join(
        local_extract_path, expected_csv_filename
    )  # ✅ Use `local_extract_path`

    if expected_csv_filename not in extracted_files:
        logging.error(f"Missing expected file: {csv_file_path}")
        return

    # **Step 3: Ensure Table Exists**
    ensure_table_exists(engine, get_metadata(), table_name, table_definition_function)

    # **Step 4: Process and Clean Data**
    df = pd.read_csv(csv_file_path)
    df = clean_data(df, column_mapping)

    # **Step 5: Insert Data**
    try:
        logging.info(f"Inserting data into table: {table_name}")
        insert_data(df, get_metadata().tables[table_name], session)
        logging.info(f"Data successfully inserted into {table_name}.")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}", exc_info=True)
    finally:
        session.close()
        logging.info("Processing completed successfully.")

    # **Step 6: Clean Up Extracted Data**
    clear_directory(local_extract_path)  # ✅ Only clean extracted files, not zip
