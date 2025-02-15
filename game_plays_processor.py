"""
August 11, 2024.
Code for reading in game_plays.csv from
S3 bucket. Create table schema. Clean and
insert data into the table. Remove csv file.
Eric Winiecke.
"""

import logging
import os
import shutil

import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
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
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

# Define table schema for game_plays
metadata = MetaData()

game_plays = Table(
    "game_plays",
    metadata,
    Column("play_id", String(20), primary_key=True),
    Column("game_id", BigInteger),
    Column("team_id_for", Integer, nullable=True),
    Column("team_id_against", Integer, nullable=True),
    Column("event", String(50)),
    Column("secondaryType", String(50)),
    Column("x", Float, nullable=True),
    Column("y", Float, nullable=True),
    Column("period", Integer),
    Column("periodType", String(50)),
    Column("periodTime", Integer),
    Column("periodTimeRemaining", Integer),
    Column("dateTime", DateTime(timezone=False)),
    Column("goals_away", Integer, nullable=True),
    Column("goals_home", Integer, nullable=True),
    Column("description", String(255)),
    Column("st_x", Integer, nullable=True),
    Column("st_y", Integer, nullable=True),
)

# Create the database engine
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


def clean_data(df, column_mapping):
    """Replace NaN with None for all columns."""
    df = df.where(pd.notnull(df), None)

    # Replace NaN with 0 for x and y columns
    df["x"] = df["x"].fillna(0)
    df["y"] = df["y"].fillna(0)

    # Truncate long strings and remove whitespace
    for column, dtype in df.dtypes.items():
        if dtype == "object":
            df[column] = df[column].apply(
                lambda x: str(x).strip()[:255] if isinstance(x, str) else x
            )

    # Convert columns to appropriate data types based on column_mapping
    for db_column, csv_column in column_mapping.items():
        if db_column in [
            "team_id_for",
            "team_id_against",
            "period",
            "periodTime",
            "periodTimeRemaining",
            "goals_away",
            "goals_home",
            "st_x",
            "st_y",
        ]:
            df[csv_column] = (
                pd.to_numeric(df[csv_column], downcast="integer", errors="coerce")
                .fillna(pd.NA)
                .astype(pd.Int64Dtype())
            )
        elif db_column in ["x", "y"]:
            df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce")
        elif db_column == "dateTime":
            df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")

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
            with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
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
    logging.info(f"Unique values in 'team_id_for': {df['team_id_for'].unique()}")
    logging.info(f"Unique values in 'team_id_against': {df['team_id_against'].unique()}")

    # Convert columns to numeric and identify problematic rows
    for column in [
        "team_id_for",
        "team_id_against",
        "period",
        "periodTime",
        "periodTimeRemaining",
    ]:
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


# Define the column mapping for game_plays.csv
game_plays_column_mapping = {
    "play_id": "play_id",
    "game_id": "game_id",
    "team_id_for": "team_id_for",
    "team_id_against": "team_id_against",
    "event": "event",
    "secondaryType": "secondaryType",
    "x": "x",
    "y": "y",
    "period": "period",
    "periodType": "periodType",
    "periodTime": "periodTime",
    "periodTimeRemaining": "periodTimeRemaining",
    "dateTime": "dateTime",
    "goals_away": "goals_away",
    "goals_home": "goals_home",
    "description": "description",
    "st_x": "st_x",
    "st_y": "st_y",
}

# S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = os.getenv("S3_FILE_KEY", "game_plays.csv.zip")

# Local paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game_plays.zip")
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

    # Path to game_plays.csv file
    game_plays_csv_file_path = os.path.join(local_extract_path, "game_plays.csv")

    # Create the table if it does not exist
    create_table(engine)

    # Process and insert game_plays.csv
    if os.path.exists(game_plays_csv_file_path):
        process_and_insert_csv(game_plays_csv_file_path, game_plays, game_plays_column_mapping)
    else:
        logging.error(f"CSV file {game_plays_csv_file_path} not found")

    # Clear the data and extracted folders after processing
    clear_directory(data_path)
    clear_directory(local_extract_path)


if __name__ == "__main__":
    main()
