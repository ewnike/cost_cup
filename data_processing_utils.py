"""
Utility functions for data processing.

This module provides common functions for:
- Cleaning and processing data
- Inspecting and validating data
- Managing database operations

Eric Winiecke
February 17, 2025
"""

import glob
import logging
import os
import re
import shutil
import string
from pathlib import Path

import boto3
import botocore
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from .db_utils import get_metadata
from .log_utils import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client("s3")


def ensure_table_exists(engine, metadata, table_name, table_definition_function):
    """Ensure a table exists in the database, create it if necessary."""
    if table_name not in metadata.tables:
        logger.info(f"Creating missing table: {table_name}")
        table_definition_function(metadata)  # Dynamically define table
        metadata.create_all(engine)
        metadata.reflect(bind=engine)  # Refresh metadata


def clear_player_cap_hits_dir(csv_dir: str, pattern: str = "player_cap_hits_*.csv") -> None:
    """Delete cap-hit CSVs after successful DB load."""
    paths = glob.glob(os.path.join(csv_dir, pattern))
    for p in paths:
        try:
            os.remove(p)
            logger.info("Deleted %s", p)
        except OSError as e:
            logger.error("Failed deleting %s: %s", p, e)


def clear_dir_patterns(directory: str, patterns: list[str]) -> None:
    """
    Delete files in `directory` matching any pattern in `patterns`.

    patterns can be exact filenames or globs, e.g.
      ["NHL_2019_team_stats.csv"] or ["NHL_*_team_stats.csv"]
    """
    for pat in patterns:
        path_pat = os.path.join(directory, pat)
        for p in glob.glob(path_pat):
            try:
                os.remove(p)
                logger.info("Deleted %s", p)
            except OSError as e:
                logger.error("Failed deleting %s: %s", p, e)


def clean_data(df: pd.DataFrame, column_mapping: dict[str, str], drop_duplicates: bool = True):
    """
    Clean and format a DataFrame using a dtype mapping.

    column_mapping = { "col_name": "int64" | "float64" | "string" | "datetime64[ns]" ... }
    """
    if df is None or df.empty:
        return df

    # Normalize headers first (Spotrac / CSVs sometimes have whitespace)
    df.columns = [c.strip() for c in df.columns]

    # Only operate on columns we actually care about (present in df)
    cols = [c for c in column_mapping.keys() if c in df.columns]

    # Don’t blindly fill numeric NaNs with 0 for these
    never_zero = {"season"}

    # 1) Basic null cleanup
    for c in cols:
        if pd.api.types.is_numeric_dtype(df[c]) and c not in never_zero:
            df[c] = df[c].fillna(0)
        else:
            df[c] = df[c].where(pd.notnull(df[c]), None)

    # 2) Apply type conversions based on dtype mapping
    for c in cols:
        dtype = str(column_mapping[c]).lower()

        if "datetime" in dtype:
            df[c] = pd.to_datetime(df[c], errors="coerce")

        elif dtype.startswith("int"):
            # integers: coerce -> fillna(0) -> int
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

        elif dtype.startswith("float"):
            df[c] = pd.to_numeric(df[c], errors="coerce")

        else:
            # treat as string
            df[c] = df[c].astype(str).str.strip()

    # 3) Optional de-dupe
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
    logger.info(f"Before Suffix Addition - Unique play_ids: {df['play_id'].nunique()}")

    play_id_counts = {}  # Dictionary to track occurrences of each play_id

    # Iterate over the DataFrame index to avoid issues with Series indexing
    for idx in df.index:
        play_id = df.at[idx, "play_id"]  # Access play_id directly by index
        logger.debug(f"Processing play_id: {play_id}")  # Debug log for each play_id

        # Check if play_id has already been seen
        if play_id in play_id_counts:
            # Increment count and add the appropriate suffix
            play_id_counts[play_id] += 1
            suffix = string.ascii_lowercase[play_id_counts[play_id] - 1]
            df.at[idx, "play_id"] = f"{play_id}{suffix}"
            logger.debug(f"Updated play_id: {df.at[idx, 'play_id']}")  # Log updated play_id
        else:
            # Initialize the count for this play_id
            play_id_counts[play_id] = 1

    #     # ✅ Log unique play_ids after processing
    logger.info(f"After Suffix Addition - Unique play_ids: {df['play_id'].nunique()}")
    print(df.tail(100))
    return df


COLUMN_RENAMES = {
    # corsi
    "CF_Percent": "cf_percent",
    "Cap_Hit": "cap_hit",
    "timeOnIce": "time_on_ice",
    "evenTimeOnIce": "even_time_on_ice",
    "shortHandedTimeOnIce": "short_handed_time_on_ice",
    "powerPlayTimeOnIce": "power_play_time_on_ice",
    "date_time_GMT": "date_time_gmt",
    "dateTime": "date_time",
    "periodTime": "period_time",
    "periodTimeRemaining": "period_time_remaining",
    # add more as needed
}


def to_snake(s: str) -> str:
    """Make constants pythonic."""
    s = s.strip()
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)  # camelCase -> snake
    return s.lower()


def normalize_columns(df):
    """Apply to_snake where appropriate."""
    df.columns = [to_snake(c) for c in df.columns]
    # apply explicit overrides after generic snake-case
    df = df.rename(columns={to_snake(k): v for k, v in COLUMN_RENAMES.items()})
    return df


#     return df
def clean_and_transform_data(df, column_mapping, table_name: str | None = None):
    """Apply transformations and clean data using dtype mapping (col -> dtype)."""
    # Normalize headers
    df.columns = [c.strip() for c in df.columns]

    # Special-case player_info header normalization (shootCatches aliases)
    if table_name == "player_info":
        df = normalize_player_info_columns(df)

    # Clean + dtype coercion (this should be the ONLY place types get coerced)
    df = clean_data(df, column_mapping, drop_duplicates=False)

    # If play_id exists, enforce uniqueness
    if "play_id" in df.columns:
        df = add_suffix_to_duplicate_play_ids(df)

    # Height conversion (if present)
    if "height" in df.columns:
        df["height"] = df["height"].apply(convert_height)

    # Drop duplicates at the end (optional)
    df = df.drop_duplicates(ignore_index=True)

    return df


def insert_data(df, table, session):
    """Insert DataFrame into a database table. Assumes df is already cleaned/coerced."""
    if df.empty:
        logger.error(f"DataFrame is empty! No data inserted into {table.name}.")
        return

    # ✅ Filter df to table schema
    valid_cols = set(table.columns.keys())
    extra_cols = [c for c in df.columns if c not in valid_cols]
    if extra_cols:
        logger.warning(f"Dropping extra columns not in {table.name}: {extra_cols}")
        df = df.drop(columns=extra_cols)

    # ✅ Ensure required NOT NULL cols exist in df
    missing_required = [
        col.name
        for col in table.columns
        if not col.nullable
        and col.default is None
        and col.server_default is None
        and col.name not in df.columns
    ]
    if missing_required:
        logger.error(f"Missing required NOT NULL columns for {table.name}: {missing_required}")
        return

    # Keep only schema columns, in any order
    df = df[[c for c in df.columns if c in valid_cols]]

    logger.info(f"Inserting {len(df)} rows into {table.name}.")
    data = df.to_dict(orient="records")

    try:
        with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
            for record in data:
                session.execute(table.insert().values(**record))
                pbar.update(1)
        session.commit()
        logger.info(f"Data successfully inserted into {table.name}.")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error inserting data into {table.name}: {e}", exc_info=True)
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error inserting into {table.name}: {e}", exc_info=True)


def clear_directory(directory):
    """Clear directory if it exists."""
    try:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            logger.info(f"Cleared directory: {directory}")
        os.makedirs(directory, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to clear directory {directory}: {e}")


def download_zip_from_s3(bucket_name, s3_file_key, local_download_path):
    """Download a ZIP file from S3 and save it as a file, ensuring correct behavior."""
    if not local_download_path:  # Check if the download path is empty
        logger.error("Download path is empty. Skipping download operation.")
        return

    # Ensuring the directory exists
    directory = os.path.dirname(local_download_path)
    if directory:  # Only attempt to create the directory if it's not empty
        os.makedirs(directory, exist_ok=True)
    else:
        logger.error("Derived directory path is empty. Cannot ensure directory existence.")
        return

    # Proceed with the download if the path checks out
    try:
        s3_client.download_file(bucket_name, s3_file_key, local_download_path)
        logger.info(f"Successfully downloaded {s3_file_key} to {local_download_path}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.error(f"File not found: {s3_file_key} in bucket {bucket_name}.")
        elif e.response["Error"]["Code"] == "403":
            logger.error(
                f"Access denied to {s3_file_key} in bucket {bucket_name}. Check permissions."
            )
        else:
            logger.error(f"Error downloading file from S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to download file from S3: {e}")
        raise


def extract_zip(zip_path, extract_to):
    """Extract a ZIP file to the specified directory, ensuring correct file structure."""
    # Check if the zip_path is provided and it exists
    if not zip_path or not os.path.exists(zip_path):
        if not zip_path:
            logger.info("No ZIP path provided; skipping extraction.")
        else:
            logger.error(f"ERROR: ZIP file not found: {zip_path}")
        return []

    # Ensure the extraction directory exists
    os.makedirs(extract_to, exist_ok=True)

    try:
        # Extract the ZIP file into `data/extracted/`
        shutil.unpack_archive(zip_path, extract_to)
        logger.info(f"Extracted {zip_path} to {extract_to}")

        # Return list of extracted files
        return os.listdir(extract_to)
    except Exception as e:
        logger.error(f"ERROR: Failed to extract {zip_path} - {e}")
        logger.info(f"{extract_to}")
        logger.info(f"{zip_path}")
        return []


def normalize_player_info_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column spellings for shootCatches."""
    # strip whitespace in headers
    df.columns = [c.strip() for c in df.columns]

    # handle known aliases
    rename_map = {}
    for c in df.columns:
        key = c.replace("_", "").replace(" ", "").strip().lower()
        if key in {"shootscatches", "shootcatches", "shootcatch"}:
            rename_map[c] = "shootCatches"
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


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

    clear_directory(config["local_extract_path"])

    download_path = (
        config["local_zip_path"]
        if config["handle_zip"]
        else os.path.join(config["local_download_path"], config["expected_csv_filename"])
    )

    download_zip_from_s3(config["bucket_name"], config["s3_file_key"], download_path)

    if config["handle_zip"]:
        extract_zip(download_path, config["local_extract_path"])

        extract_dir = Path(config["local_extract_path"])
        target = config["expected_csv_filename"]

        matches = [
            p
            for p in extract_dir.rglob(target)
            if "__MACOSX" not in p.parts and not p.name.startswith("._")
        ]

        if not matches:
            extracted_files = [
                str(p.relative_to(extract_dir))
                for p in extract_dir.rglob("*")
                if p.is_file() and "__MACOSX" not in p.parts and not p.name.startswith("._")
            ]
            logger.error(
                f"Extracted file not found after extraction: {target}. "
                f"Example extracted files: {extracted_files[:50]}"
            )
            return

        csv_file_path = str(matches[0])
        clear_directory(config["local_download_path"])

    else:
        csv_file_path = download_path
        if not os.path.exists(csv_file_path):
            logger.error(f"Downloaded file not found at path: {csv_file_path}")
            return

    try:
        if csv_file_path.endswith(".csv") or csv_file_path.endswith(".csv.xls"):
            df = pd.read_csv(csv_file_path)
        else:
            df = pd.read_excel(csv_file_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Error reading file {csv_file_path}: {e}")
        return

    # ✅ table-specific fixes BEFORE cleaning
    if config["table_name"].startswith("raw_shifts") and "shift_num" in df.columns:
        df["shift_num"] = pd.to_numeric(df["shift_num"], errors="coerce").fillna(0).astype(int)

    # ✅ Ensure the table exists BEFORE referencing metadata.tables[...]
    ensure_table_exists(
        config["engine"],
        get_metadata(),
        config["table_name"],
        config["table_definition_function"],
    )

    # ✅ Clean after table fixes
    df = clean_and_transform_data(df, config["column_mapping"], table_name=config["table_name"])

    try:
        logger.info(f"Inserting data into table: {config['table_name']}")
        insert_data(df, get_metadata().tables[config["table_name"]], session)
        logger.info(f"Data successfully inserted into {config['table_name']}.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting data into {config['table_name']}: {e}", exc_info=True)
    finally:
        session.close()
