# """
# Utility functions for data processing.

# This module provides common functions for:
# - Cleaning and processing data
# - Inspecting and validating data
# - Managing database operations

# Eric Winiecke
# February 17, 2025
# """

# import logging
# import os
# import shutil

# import boto3
# import botocore
# import pandas as pd
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.orm import sessionmaker
# from tqdm import tqdm

# from db_utils import (
#     define_game_plays_players_test,
#     define_game_plays_processor_test,
#     define_game_processor_test,
# )

# # Configure logging
# logging.basicConfig(
#     filename="data_processing.log",
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )

# # Initialize S3 client
# s3_client = boto3.client("s3")


# def ensure_table_exists(engine, metadata, table_name):
#     """Ensure a table exists in the database, create it if necessary."""
#     # Check if the table exists
#     if table_name not in metadata.tables:
#         logging.info(f"Creating missing table: {table_name}")

#         # Dynamically call the correct table definition function
#         if table_name == "game_plays_processor_test":
#             define_game_plays_processor_test(metadata)
#         elif table_name == "game_plays_players_test":
#             define_game_plays_players_test(metadata)
#         elif table_name == "game_processor_test":
#             define_game_processor_test(metadata)
#         else:
#             raise ValueError(f"Table definition for {table_name} is not available.")

#         # Create the table
#         metadata.create_all(engine)
#         metadata.reflect(bind=engine)  # Refresh metadata


# def clean_data(df, column_mapping):
#     """Generically clean and format a DataFrame based on column mappings."""
#     # Replace NaN values with None
#     df = df.where(pd.notnull(df), None)

#     # Trim whitespace for string columns
#     for column in df.select_dtypes(include=["object"]).columns:
#         df[column] = df[column].str.strip()

#     # Convert columns to appropriate data types based on column_mapping
#     for db_column, csv_column in column_mapping.items():
#         if csv_column in df.columns:
#             if db_column in ["x", "y"]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce").fillna(0)
#             elif db_column == "dateTime":
#                 df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
#             elif db_column in [
#                 "game_id",
#                 "player_id",
#                 "team_id_for",
#                 "team_id_against",
#                 "period",
#             ]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], downcast="integer", errors="coerce")
#             else:
#                 df[csv_column] = (
#                     df[csv_column].astype(str).str.strip()
#                 )  # Convert non-numeric columns to string

#     return df


# def inspect_data(df, numeric_columns):
#     """
#     Inspect a DataFrame for numerical column issues.

#     Args:
#     ----
#         df (pd.DataFrame): The input DataFrame.
#         numeric_columns (list): List of numeric columns to check.

#     Returns:
#     -------
#         None

#     """
#     for column in numeric_columns:
#         if column in df.columns:
#             df[column] = pd.to_numeric(df[column], errors="coerce")
#             logging.warning(f"Rows with large values in {column}:")
#             logging.warning(df[df[column] > 2147483647])
#             logging.warning(f"Rows with negative values in {column}:")
#             logging.warning(df[df[column] < 0])


# def insert_data(df, table, session):
#     """Insert DataFrame into the database table."""
#     if df.shape[0] == 0:
#         logging.error(f"DataFrame is empty! No data inserted into {table.name}.")
#         return  # Avoid inserting if DataFrame is empty

#     logging.info(f"Inserting data into {table.name}: {df.shape[0]} rows.")

#     data = df.to_dict(orient="records")

#     try:
#         with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
#             for record in data:
#                 session.execute(table.insert().values(**record))
#                 pbar.update(1)

#         session.commit()  # Ensure changes are saved
#         logging.info(f"Data successfully inserted into {table.name}.")
#     except SQLAlchemyError as e:
#         session.rollback()  # Prevent partial inserts
#         logging.error(f"Error inserting data into {table.name}: {e}", exc_info=True)
#     except Exception as e:
#         logging.error(f"Unexpected error inserting into {table.name}: {e}", exc_info=True)
#     finally:
#         session.close()  # Ensure session closes


# def clear_directory(directory):
#     """Clear a directory and recreate it."""
#     if os.path.exists(directory):
#         shutil.rmtree(directory)
#         logging.info(f"Cleared directory: {directory}")
#     os.makedirs(directory, exist_ok=True)


# def download_zip_from_s3(bucket, key, download_path):
#     """Download a zip file from S3."""
#     logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
#     try:
#         s3_client.download_file(bucket, key, download_path)
#         logging.info(f"Downloaded {key} from S3 to {download_path}")
#     except botocore.exceptions.ClientError as e:
#         logging.error(f"Error downloading file from S3: {e}")
#         if e.response["Error"]["Code"] == "404":
#             logging.error(f"ERROR: The object {key} does not exist in bucket {bucket}.")
#         else:
#             raise


# def extract_zip(zip_path, extract_to):
#     """Extract zip files."""
#     if not os.path.exists(zip_path):
#         logging.error(f"ERROR: ZIP file not found: {zip_path}")
#         return []

#     try:
#         shutil.unpack_archive(zip_path, extract_to)
#         logging.info(f"Extracted {zip_path} to {extract_to}")
#         return os.listdir(extract_to)
#     except Exception as e:
#         logging.error(f"ERROR: Failed to extract {zip_path} - {e}")
#         return []


# def process_and_insert_csv(csv_file_path, table, column_mapping, engine):
#     """
#     Process and insert data from a CSV file into a database table.

#     Parameters
#     ----------
#     csv_file_path : str
#         Path to the CSV file.
#     table : sqlalchemy.Table
#         SQLAlchemy Table object representing the database table.
#     column_mapping : dict
#         Mapping of CSV column names to database column names.
#     engine : sqlalchemy.engine.Engine
#         SQLAlchemy engine instance.

#     Returns
#     -------
#     None

#     """
#     try:
#         logging.info(f"Processing {csv_file_path} for table {table.name}")
#         df = pd.read_csv(csv_file_path)
#         logging.info(f"DataFrame for {table.name} loaded with {len(df)} records")

#         # Inspect and clean data
#         numeric_columns = column_mapping.keys()
#         inspect_data(df, numeric_columns)
#         df = clean_data(df, numeric_columns)

#         # Clear existing data
#         with engine.connect() as connection:
#             connection.execute(table.delete())
#             connection.commit()
#             logging.info(f"Cleared existing data in {table.name}")

#         # Insert cleaned data
#         Session = sessionmaker(bind=engine)  # pylint: disable=invalid-name
#         session = Session()
#         insert_data(df, table, session)

#     except FileNotFoundError as e:
#         logging.error(f"File not found: {csv_file_path} - {e}")
#     except Exception as e:
#         logging.error(f"Error processing CSV file {csv_file_path}: {e}")


# def insert_data_from_csv(engine, table_name, file_path, delete_after=True):
#     """
#     Insert data from a CSV file into the specified database table.

#     Args:
#     ----
#         engine (sqlalchemy.Engine): The database engine.
#         table_name (str): The name of the table to insert data into.
#         file_path (str): The path to the CSV file.
#         delete_after (bool): If True, deletes the file after insertion.

#     Raises:
#     ------
#         SQLAlchemyError: If an error occurs while inserting data.
#         FileNotFoundError: If the CSV file is not found.

#     """
#     # Create session factory, though not needed for `to_sql()`
#     session_factory = sessionmaker(bind=engine)  # Instead of "Session"
#     session = session_factory()  # Instead of "Session()"

#     try:
#         # Load CSV into DataFrame
#         df = pd.read_csv(file_path)

#         if df.empty:
#             print(f"Skipping {file_path}: No data to insert.")
#             return

#         # Insert data into the database
#         df.to_sql(table_name, con=engine, if_exists="append", index=False)
#         print(f"Data inserted successfully into {table_name}")

#         # Delete file after successful insertion (if enabled)
#         if delete_after:
#             os.remove(file_path)
#             print(f"File {file_path} deleted successfully.")

#     except FileNotFoundError as e:
#         print(f"File not found: {file_path} - {e}")

#     except SQLAlchemyError as e:
#         print(f"SQLAlchemy Error inserting data into {table_name}: {e}")

#     except Exception as e:
#         print(f"Unexpected error processing file '{file_path}': {e}")

#     finally:
#         session.close()


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

from db_utils import get_db_engine, get_metadata

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


# def clean_data(df, column_mapping):
#     """Clean and format a DataFrame based on column mappings."""
#     df = df.where(pd.notnull(df), None)  # Replace NaN with None
#     for column in df.select_dtypes(include=["object"]).columns:
#         df[column] = df[column].str.strip()

#     for db_column, csv_column in column_mapping.items():
#         if csv_column in df.columns:
#             if db_column in ["x", "y"]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce").fillna(0)
#             elif db_column == "dateTime":
#                 df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
#             elif db_column in ["game_id", "player_id", "team_id_for", "team_id_against", "period"]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], downcast="integer", errors="coerce")
#             else:
#                 df[csv_column] = df[csv_column].astype(str).str.strip()
#     return df


# def clean_data(df, column_mapping):
#     """Clean and format a DataFrame based on column mappings."""
#     # Replace NaN values with 0 for integer and float columns
#     for col in df.columns:
#         if df[col].dtype.kind in "fi":  # Float or Integer
#             df[col] = df[col].fillna(0)  # Convert NaN to 0
#         else:
#             df[col] = df[col].where(pd.notnull(df[col]), None)  # Keep None for strings

#     # Convert columns to appropriate data types based on column_mapping
#     for db_column, csv_column in column_mapping.items():
#         if csv_column in df.columns:
#             if db_column in ["x", "y"]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce").fillna(0)
#             elif db_column == "dateTime":
#                 df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
#             elif db_column in [
#                 "game_id",
#                 "player_id",
#                 "team_id_for",
#                 "team_id_against",
#                 "period",
#                 "timeOnIce",
#                 "assists",
#                 "goals",
#                 "shots",
#                 "hits",
#                 "powerPlayGoals",
#                 "powerPlayAssists",
#                 "penaltyMinutes",
#                 "faceOffWins",
#                 "faceoffTaken",
#                 "takeaways",
#                 "giveaways",
#                 "shortHandedGoals",
#                 "shortHandedAssists",
#                 "blocked",
#                 "plusMinus",
#                 "evenTimeOnIce",
#                 "shortHandedTimeOnIce",
#                 "powerPlayTimeOnIce",
#             ]:
#                 df[csv_column] = pd.to_numeric(
#                     df[csv_column], downcast="integer", errors="coerce"
#                 ).fillna(0)
#             else:
#                 df[csv_column] = df[csv_column].astype(str).str.strip()

#     return df


# def clean_data(df, column_mapping):
#     """Clean and format a DataFrame based on column mappings."""
#     # Replace NaN values with 0 for integer and float columns
#     for col in df.columns:
#         if df[col].dtype.kind in "fi":  # Float or Integer
#             df[col] = df[col].fillna(0)  # Convert NaN to 0
#         else:
#             df[col] = df[col].where(pd.notnull(df[col]), None)  # Keep None for strings

#     # Convert columns to appropriate data types based on column_mapping
#     for db_column, csv_column in column_mapping.items():
#         if csv_column in df.columns:
#             if db_column in ["x", "y"]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce").fillna(0)
#             elif db_column == "dateTime":
#                 df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
#             elif db_column in [
#                 "game_id",
#                 "player_id",
#                 "team_id_for",
#                 "team_id_against",
#                 "period",
#                 "timeOnIce",
#                 "assists",
#                 "goals",
#                 "shots",
#                 "hits",
#                 "powerPlayGoals",
#                 "powerPlayAssists",
#                 "penaltyMinutes",
#                 "faceOffWins",
#                 "faceoffTaken",
#                 "takeaways",
#                 "giveaways",
#                 "shortHandedGoals",
#                 "shortHandedAssists",
#                 "blocked",
#                 "plusMinus",
#                 "evenTimeOnIce",
#                 "shortHandedTimeOnIce",
#                 "powerPlayTimeOnIce",
#             ]:
#                 df[csv_column] = pd.to_numeric(
#                     df[csv_column], downcast="integer", errors="coerce"
#                 ).fillna(0)
#             else:
#                 df[csv_column] = df[csv_column].astype(str).str.strip()

#     # ✅ Drop duplicates before returning the cleaned DataFrame
#     initial_row_count = len(df)
#     df = df.drop_duplicates(ignore_index=True)
#     logging.info(f"Dropped {initial_row_count - len(df)} duplicate rows.")


#     return df
# def clean_data(df, column_mapping):
#     """Clean and format a DataFrame based on column mappings."""
#     # Replace NaN values with 0 for integer and float columns
#     for col in df.columns:
#         if df[col].dtype.kind in "fi":  # Float or Integer
#             df[col] = df[col].fillna(0)  # Convert NaN to 0
#         else:
#             df[col] = df[col].where(pd.notnull(df[col]), None)  # Keep None for strings

#     # Convert columns to appropriate data types based on column_mapping
#     for db_column, csv_column in column_mapping.items():
#         if csv_column in df.columns:
#             if db_column in ["x", "y"]:
#                 df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce").fillna(0)
#             elif db_column == "dateTime":
#                 df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
#             elif db_column in [
#                 "game_id",
#                 "player_id",
#                 "team_id_for",
#                 "team_id_against",
#                 "period",
#                 "timeOnIce",
#                 "assists",
#                 "goals",
#                 "shots",
#                 "hits",
#                 "powerPlayGoals",
#                 "powerPlayAssists",
#                 "penaltyMinutes",
#                 "faceOffWins",
#                 "faceoffTaken",
#                 "takeaways",
#                 "giveaways",
#                 "shortHandedGoals",
#                 "shortHandedAssists",
#                 "blocked",
#                 "plusMinus",
#                 "evenTimeOnIce",
#                 "shortHandedTimeOnIce",
#                 "powerPlayTimeOnIce",
#             ]:
#                 df[csv_column] = pd.to_numeric(
#                     df[csv_column], downcast="integer", errors="coerce"
#                 ).fillna(0)
#             else:
#                 df[csv_column] = df[csv_column].astype(str).str.strip()

#     # ✅ Drop Duplicates Based on Primary Key (e.g., 'play_id' in game_plays)
#     if "play_id" in df.columns:
#         initial_row_count = len(df)
#         df = df.drop_duplicates(subset=["play_id"], ignore_index=True)
#         logging.info(f"Dropped {initial_row_count - len(df)} duplicate rows based on 'play_id'.")


#     return df


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


# def download_zip_from_s3(bucket, key, download_path):
#     """Download a zip file from S3."""
#     logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
#     try:
#         s3_client.download_file(bucket, key, download_path)
#         logging.info(f"Downloaded {key} from S3 to {download_path}")
#     except botocore.exceptions.ClientError as e:
#         logging.error(f"Error downloading file from S3: {e}")
#         if e.response["Error"]["Code"] == "404":
#             logging.error(f"ERROR: The object {key} does not exist in bucket {bucket}.")
#         else:
#             raise
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


# def extract_zip(zip_path, extract_to):
#     """Extract zip files."""
#     if not os.path.exists(zip_path):
#         logging.error(f"ERROR: ZIP file not found: {zip_path}")
#         return []

#     try:
#         shutil.unpack_archive(zip_path, extract_to)
#         logging.info(f"Extracted {zip_path} to {extract_to}")
#         return os.listdir(extract_to)
#     except Exception as e:
#         logging.error(f"ERROR: Failed to extract {zip_path} - {e}")
#         return []


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


# def process_and_insert_data(
#     bucket_name,
#     s3_file_key,
#     local_zip_path,
#     expected_csv_filename,
#     table_definition_function,
#     table_name,
#     column_mapping,
#     engine,
# ):
#     """
#     Download, extract, clean, and insert data into a database table.

#     Args:
#     ----
#         bucket_name (str): The S3 bucket name.
#         s3_file_key (str): The file key in S3.
#         local_zip_path (str): The path to save the downloaded zip file.
#         expected_csv_filename (str): The expected CSV file name inside the extracted files.
#         table_definition_function (function): Function to define the table schema.
#         table_name (str): The target database table name.
#         column_mapping (dict): Column mapping for data cleaning.
#         engine (sqlalchemy.engine.Engine): The database engine instance.

#     """
#     session_factory = sessionmaker(bind=engine)  # Use snake_case
#     session = session_factory()

#     # **Step 1: Clear Old Data**
#     clear_directory(local_zip_path)

#     # **Step 2: Download and Extract Data**
#     download_zip_from_s3(bucket_name, s3_file_key, local_zip_path)
#     extracted_files = extract_zip(local_zip_path, local_zip_path)
#     logging.info(f"Extracted files: {extracted_files}")

#     csv_file_path = os.path.join(local_zip_path, expected_csv_filename)

#     if expected_csv_filename not in extracted_files:
#         logging.error(f"Missing expected file: {csv_file_path}")
#         return

#     # **Step 3: Ensure Table Exists**
#     ensure_table_exists(engine, get_metadata(), table_name, table_definition_function)

#     # **Step 4: Process and Clean Data**
#     df = pd.read_csv(csv_file_path)
#     df = clean_data(df, column_mapping)

#     # **Step 5: Insert Data**
#     try:
#         logging.info(f"Inserting data into table: {table_name}")
#         insert_data(df, get_metadata().tables[table_name], session)
#         logging.info(f"Data successfully inserted into {table_name}.")
#     except Exception as e:
#         logging.error(f"Error inserting data into {table_name}: {e}", exc_info=True)
#     finally:
#         session.close()
#         logging.info("Processing completed successfully.")


#     # **Step 6: Clean Up Extracted Data**
#     clear_directory(local_zip_path)
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
