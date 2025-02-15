"""
August 11, 2024
Code to process game_shifts data.
Upload from AWS S3 Bucket. Read data.
Insert data into a data table in hockey_stats db.
Eric Winiecke.
"""

import logging
import os
import shutil

import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5432))
DATABASE = os.getenv("DATABASE", "hockey_stats")

# Create the connection string
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)

# Define table schema for game_shifts
metadata = MetaData()

game_shifts = Table(
    "game_shifts",
    metadata,
    Column("game_id", BigInteger),
    Column("player_id", BigInteger),
    Column("period", Integer),
    Column("shift_start", Integer),
    Column("shift_end", Integer),
)

# Create the database engine
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


def clean_data(df, column_mapping):
    """Function to clean the DataFrame."""
    # Replace NaN with None for all columns
    df = df.where(pd.notnull(df), None)

    # Convert columns to appropriate data types based on column_mapping
    for db_column, csv_column in column_mapping.items():
        if db_column in ["game_id", "player_id", "period", "shift_start", "shift_end"]:
            df[csv_column] = (
                pd.to_numeric(df[csv_column], errors="coerce")
                .fillna(pd.NA)
                .astype(pd.Int64Dtype())  # Ensures the type is integer with NA support
            )

    # Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    # Print rows where integer values are out of range
    for column in df.select_dtypes(include=["Int64"]).columns:
        if df[column].max() > 2147483647 or df[column].min() < -2147483648:
            logging.warning(f"Rows with out of range values in column '{column}':")
            logging.warning(df[(df[column] > 2147483647) | (df[column] < -2147483648)])

    return df


def create_table(engine):
    """Function to create the table if it does not exist."""
    metadata.create_all(engine)


def clear_table(engine, table):
    """Function to clear the table if it exists."""
    with engine.connect() as connection:
        connection.execute(table.delete())
        connection.commit()
        logging.info(f"Table {table.name} cleared.")


def insert_data(df, table):
    """Function to insert data into the database."""
    data = df.to_dict(orient="records")
    with Session() as session:
        try:
            with tqdm(
                total=len(data), desc=f"Inserting data into {table.name}"
            ) as pbar:
                for record in data:
                    session.execute(table.insert().values(**record))
                    session.commit()
                    pbar.update(1)
            logging.info(f"Data inserted successfully into {table.name}")
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"Error inserting data into {table.name}: {e}")


def inspect_data(df):
    """Function to inspect data for errors."""
    # Check unique values in critical columns
    logging.info(f"Unique values in 'game_id': {df['game_id'].unique()}")
    logging.info(f"Unique values in 'player_id': {df['player_id'].unique()}")

    # Convert columns to numeric and identify problematic rows
    for column in ["game_id", "player_id", "period", "shift_start", "shift_end"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        logging.warning(f"Rows with large values in {column}:")
        logging.warning(df[df[column] > 1000])
        logging.warning(f"Rows with negative values in {column}:")
        logging.warning(df[df[column] < 0])


def process_and_insert_csv(csv_file_path, table, column_mapping):
    """Function to process and insert data from a CSV file."""
    try:
        logging.info(f"Processing {csv_file_path} for table {table.name}")
        df = pd.read_csv(csv_file_path)
        logging.info(f"DataFrame for {table.name} loaded with {len(df)} records")

        # Print the datatype of each column
        logging.info("Column Datatypes:")
        logging.info(df.dtypes)

        # Print a sample of the data to debug
        logging.info("Sample data:")
        logging.info(df.head())

        # Inspect data for potential errors
        inspect_data(df)

        # Clean the data
        df = clean_data(df, column_mapping)
        logging.info(f"DataFrame for {table.name} cleaned with {len(df)} records")

        # Print a sample of the cleaned data to debug
        logging.info("Cleaned sample data:")
        logging.info(df.head())

        # Clear the table if it exists
        clear_table(engine, table)

        # Insert the cleaned data into the database
        insert_data(df, table)

    except FileNotFoundError as e:
        logging.error(f"File not found: {csv_file_path} - {e}")


# Define the column mapping for game_shifts.csv
game_shifts_column_mapping = {
    "game_id": "game_id",
    "player_id": "player_id",
    "period": "period",
    "shift_start": "shift_start",
    "shift_end": "shift_end",
}

# S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = os.getenv("S3_FILE_KEY", "game_shifts.csv.zip")

# Local paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game_shifts.zip")
data_path = os.getenv("DATA_PATH", "data")  # Path to the data folder


def download_zip_from_s3(bucket, key, download_path):
    """Function to download a zip file from S3."""
    logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
    try:
        s3_client.download_file(bucket, key, download_path)
        logging.info(f"Downloaded {key} from S3 to {download_path}")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error: {e}")
        if e.response["Error"]["Code"] == "404":
            logging.error("The object does not exist.")
        else:
            raise


def extract_zip(zip_path, extract_to):
    """Function to extract zip files."""
    shutil.unpack_archive(zip_path, extract_to)
    logging.info(f"Extracted {zip_path} to {extract_to}")
    return os.listdir(extract_to)


def clear_directory(directory):
    """Function to clear a directory."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Cleared directory: {directory}")
    os.makedirs(directory, exist_ok=True)


def main():
    """Main function to handle the workflow."""
    # Clear the extracted folder and recreate it
    clear_directory(local_extract_path)

    # Download and extract the zip file from S3
    download_zip_from_s3(bucket_name, s3_file_key, local_zip_path)
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    # Path to game_shifts.csv file
    game_shifts_csv_file_path = os.path.join(local_extract_path, "game_shifts.csv")

    # Create the table if it does not exist
    create_table(engine)

    # Process and insert game_shifts.csv
    if os.path.exists(game_shifts_csv_file_path):
        process_and_insert_csv(
            game_shifts_csv_file_path, game_shifts, game_shifts_column_mapping
        )
    else:
        logging.error(f"CSV file {game_shifts_csv_file_path} not found")

    # Clear the data and extracted folders after processing
    clear_directory(data_path)
    clear_directory(local_extract_path)


if __name__ == "__main__":
    main()
