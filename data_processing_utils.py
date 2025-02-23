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

from db_utils import (
    define_game_plays_players_test,
    define_game_plays_processor_test,
    define_game_processor_test,
)

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Initialize S3 client
s3_client = boto3.client("s3")


def ensure_table_exists(engine, metadata, table_name):
    """Ensure a table exists in the database, create it if necessary."""
    # Check if the table exists
    if table_name not in metadata.tables:
        logging.info(f"Creating missing table: {table_name}")

        # Dynamically call the correct table definition function
        if table_name == "game_plays_processor_test":
            define_game_plays_processor_test(metadata)
        elif table_name == "game_plays_players_test":
            define_game_plays_players_test(metadata)
        elif table_name == "game_processor_test":
            define_game_processor_test(metadata)
        else:
            raise ValueError(f"Table definition for {table_name} is not available.")

        # Create the table
        metadata.create_all(engine)
        metadata.reflect(bind=engine)  # Refresh metadata


def clean_data(df, column_mapping):
    """Generically clean and format a DataFrame based on column mappings."""
    # Replace NaN values with None
    df = df.where(pd.notnull(df), None)

    # Trim whitespace for string columns
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].str.strip()

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
            ]:
                df[csv_column] = pd.to_numeric(df[csv_column], downcast="integer", errors="coerce")
            else:
                df[csv_column] = (
                    df[csv_column].astype(str).str.strip()
                )  # Convert non-numeric columns to string

    return df


def inspect_data(df, numeric_columns):
    """
    Inspect a DataFrame for numerical column issues.

    Args:
    ----
        df (pd.DataFrame): The input DataFrame.
        numeric_columns (list): List of numeric columns to check.

    Returns:
    -------
        None

    """
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
            logging.warning(f"Rows with large values in {column}:")
            logging.warning(df[df[column] > 2147483647])
            logging.warning(f"Rows with negative values in {column}:")
            logging.warning(df[df[column] < 0])


def insert_data(df, table, session):
    """Insert DataFrame into the database table."""
    if df.shape[0] == 0:
        logging.error(f"DataFrame is empty! No data inserted into {table.name}.")
        return  # Avoid inserting if DataFrame is empty

    logging.info(f"Inserting data into {table.name}: {df.shape[0]} rows.")

    data = df.to_dict(orient="records")

    try:
        with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
            for record in data:
                session.execute(table.insert().values(**record))
                pbar.update(1)

        session.commit()  # Ensure changes are saved
        logging.info(f"Data successfully inserted into {table.name}.")
    except SQLAlchemyError as e:
        session.rollback()  # Prevent partial inserts
        logging.error(f"Error inserting data into {table.name}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected error inserting into {table.name}: {e}", exc_info=True)
    finally:
        session.close()  # Ensure session closes


def clear_directory(directory):
    """Clear a directory and recreate it."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Cleared directory: {directory}")
    os.makedirs(directory, exist_ok=True)


def download_zip_from_s3(bucket, key, download_path):
    """Download a zip file from S3."""
    logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
    try:
        s3_client.download_file(bucket, key, download_path)
        logging.info(f"Downloaded {key} from S3 to {download_path}")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error downloading file from S3: {e}")
        if e.response["Error"]["Code"] == "404":
            logging.error(f"ERROR: The object {key} does not exist in bucket {bucket}.")
        else:
            raise


def extract_zip(zip_path, extract_to):
    """Extract zip files."""
    if not os.path.exists(zip_path):
        logging.error(f"ERROR: ZIP file not found: {zip_path}")
        return []

    try:
        shutil.unpack_archive(zip_path, extract_to)
        logging.info(f"Extracted {zip_path} to {extract_to}")
        return os.listdir(extract_to)
    except Exception as e:
        logging.error(f"ERROR: Failed to extract {zip_path} - {e}")
        return []


def process_and_insert_csv(csv_file_path, table, column_mapping, engine):
    """
    Process and insert data from a CSV file into a database table.

    Parameters
    ----------
    csv_file_path : str
        Path to the CSV file.
    table : sqlalchemy.Table
        SQLAlchemy Table object representing the database table.
    column_mapping : dict
        Mapping of CSV column names to database column names.
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine instance.

    Returns
    -------
    None

    """
    try:
        logging.info(f"Processing {csv_file_path} for table {table.name}")
        df = pd.read_csv(csv_file_path)
        logging.info(f"DataFrame for {table.name} loaded with {len(df)} records")

        # Inspect and clean data
        numeric_columns = column_mapping.keys()
        inspect_data(df, numeric_columns)
        df = clean_data(df, numeric_columns)

        # Clear existing data
        with engine.connect() as connection:
            connection.execute(table.delete())
            connection.commit()
            logging.info(f"Cleared existing data in {table.name}")

        # Insert cleaned data
        Session = sessionmaker(bind=engine)
        session = Session()
        insert_data(df, table, session)

    except FileNotFoundError as e:
        logging.error(f"File not found: {csv_file_path} - {e}")
    except Exception as e:
        logging.error(f"Error processing CSV file {csv_file_path}: {e}")
