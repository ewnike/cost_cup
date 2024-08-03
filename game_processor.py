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
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)

# Define table schemas
metadata = MetaData()

game = Table(
    "game",
    metadata,
    Column("game_id", BigInteger, primary_key=True),
    Column("season", Integer),
    Column("type", String(50)),
    Column("date_time_GMT", DateTime(timezone=True)),
    Column("away_team_id", Integer),
    Column("home_team_id", Integer),
    Column("away_goals", Integer),
    Column("home_goals", Integer),
    Column("outcome", String(100)),
    Column("home_rink_side_start", String(100)),
    Column("venue", String(100)),
    Column("venue_link", String(100)),
    Column("venue_time_zone_id", String(50)),
    Column("venue_time_zone_offset", Integer),
    Column("venue_time_zone_tz", String(25)),
)

# Create the database engine
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


# Function to clean the DataFrame
def clean_data(df, column_mapping):
    # Replace NaN with None for all columns
    df = df.where(pd.notnull(df), None)

    # Truncate long strings and remove whitespace
    for column, dtype in df.dtypes.items():
        if dtype == "object":
            df[column] = df[column].apply(
                lambda x: str(x).strip()[:255] if isinstance(x, str) else x
            )

    # Convert columns to appropriate data types based on column_mapping
    for db_column, csv_column in column_mapping.items():
        if "int" in str(df[csv_column].dtype):
            df[csv_column] = pd.to_numeric(
                df[csv_column], downcast="integer", errors="coerce"
            )
        elif "float" in str(df[csv_column].dtype):
            df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce")
        elif "date_time" in csv_column:
            df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")

    # Remove duplicates
    df = df.drop_duplicates(ignore_index=True)

    return df


# Function to create the table if it does not exist
def create_table(engine):
    metadata.create_all(engine)


# Function to clear the table if it exists
def clear_table(engine, table):
    with engine.connect() as connection:
        connection.execute(table.delete())
        connection.commit()
        logging.info(f"Table {table.name} cleared.")


# Function to insert data into the database
def insert_data(df, table):
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


# Function to inspect data for errors
def inspect_data(df):
    # Check unique values in critical columns
    logging.info(f"Unique values in 'game_id': {df['game_id'].unique()}")
    logging.info(f"Unique values in 'away_team_id': {df['away_team_id'].unique()}")
    logging.info(f"Unique values in 'home_team_id': {df['home_team_id'].unique()}")

    # Convert columns to numeric and identify problematic rows
    for column in ["game_id", "away_team_id", "home_team_id"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        logging.warning(f"Rows with large values in {column}:")
        logging.warning(df[df[column] > 1000])
        logging.warning(f"Rows with negative values in {column}:")
        logging.warning(df[df[column] < 0])


# Function to process and insert data from a CSV file
def process_and_insert_csv(csv_file_path, table, column_mapping):
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


# Define the column mapping for game.csv
column_mapping = {
    "game_id": "game_id",
    "season": "season",
    "type": "type",
    "date_time_GMT": "date_time_GMT",
    "away_team_id": "away_team_id",
    "home_team_id": "home_team_id",
    "away_goals": "away_goals",
    "home_goals": "home_goals",
    "outcome": "outcome",
    "home_rink_side_start": "home_rink_side_start",
    "venue": "venue",
    "venue_link": "venue_link",
    "venue_time_zone_id": "venue_time_zone_id",
    "venue_time_zone_offset": "venue_time_zone_offset",
    "venue_time_zone_tz": "venue_time_zone_tz",
}

# S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = os.getenv("S3_FILE_KEY", "game.csv.zip")

# Local paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_zip_path = os.path.join(local_extract_path, "game.zip")
data_path = os.getenv("DATA_PATH", "data")  # Path to the data folder


# Function to download a zip file from S3
def download_zip_from_s3(bucket, key, download_path):
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


# Function to extract zip files
def extract_zip(zip_path, extract_to):
    shutil.unpack_archive(zip_path, extract_to)
    logging.info(f"Extracted {zip_path} to {extract_to}")
    return os.listdir(extract_to)


# Function to clear a directory
def clear_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Cleared directory: {directory}")
    os.makedirs(directory, exist_ok=True)


# Main function to handle the workflow
def main():
    # Clear the extracted folder and recreate it
    clear_directory(local_extract_path)

    # Download and extract the zip file from S3
    download_zip_from_s3(bucket_name, s3_file_key, local_zip_path)
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    logging.info(f"Extracted files: {extracted_files}")

    # Path to game.csv file
    game_csv_file_path = os.path.join(local_extract_path, "game.csv")

    # Create the table if it does not exist
    create_table(engine)

    # Process and insert game.csv
    if os.path.exists(game_csv_file_path):
        process_and_insert_csv(game_csv_file_path, game, column_mapping)
    else:
        logging.error(f"CSV file {game_csv_file_path} not found")

    # Clear the data and extracted folders after processing
    clear_directory(data_path)
    clear_directory(local_extract_path)


if __name__ == "__main__":
    main()
