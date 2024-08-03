import os
import shutil

import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker

from db_utils import (
    clean_data,
    #download_and_extract_zip_from_s3,
    insert_data,
    inspect_data,
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


# Function to download a zip file from S3
def download_zip_from_s3(bucket, key, download_path):
    print(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
    try:
        s3_client.download_file(bucket, key, download_path)
        print(f"Downloaded {key} from S3 to {download_path}")
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")
        if e.response["Error"]["Code"] == "404":
            print("The object does not exist.")
        else:
            raise


# Function to extract zip files
def extract_zip(zip_path, extract_to):
    shutil.unpack_archive(zip_path, extract_to)
    print(f"Extracted {zip_path} to {extract_to}")
    return os.listdir(extract_to)


# Main function to handle the workflow
def main():
    # Remove the extracted folder and recreate it
    if os.path.exists(local_extract_path):
        shutil.rmtree(local_extract_path)
    os.makedirs(local_extract_path, exist_ok=True)

    # Download and extract the zip file from S3
    download_zip_from_s3(bucket_name, s3_file_key, local_zip_path)
    extracted_files = extract_zip(local_zip_path, local_extract_path)
    print(f"Extracted files: {extracted_files}")

    # Path to game_shifts.csv file
    game_shifts_csv_file_path = os.path.join(local_extract_path, "game_shifts.csv")

    # Create the table if it does not exist
    metadata.create_all(engine, checkfirst=True)

    # Process and insert game_shifts.csv
    if os.path.exists(game_shifts_csv_file_path):
        process_and_insert_csv(
            game_shifts_csv_file_path, game_shifts, game_shifts_column_mapping
        )
    else:
        print(f"CSV file {game_shifts_csv_file_path} not found")


def process_and_insert_csv(csv_file_path, table, column_mapping):
    try:
        print(f"Processing {csv_file_path} for table {table.name}")
        df = pd.read_csv(csv_file_path)
        print(f"DataFrame for {table.name} loaded with {len(df)} records")

        # Print the datatype of each column
        print("Column Datatypes:")
        print(df.dtypes)

        # Print a sample of the data to debug
        print("Sample data:")
        print(df.head())

        # Inspect data for potential errors
        inspect_data(df)

        # Clean the data
        df = clean_data(df, column_mapping)
        print(f"DataFrame for {table.name} cleaned with {len(df)} records")

        # Print a sample of the cleaned data to debug
        print("Cleaned sample data:")
        print(df.head())

        # Insert the cleaned data into the database
        insert_data(df, table, Session())

    except FileNotFoundError as e:
        print(f"File not found: {csv_file_path} - {e}")


if __name__ == "__main__":
    main()
