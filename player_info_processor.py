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
    Date,
    Float,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.orm import sessionmaker

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

# S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_file_key = os.getenv("S3_FILE_KEY", "player_info.csv.xls")

# Local paths
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
local_csv_path = os.path.join(local_extract_path, "player_info.csv.xls")
data_path = os.getenv("DATA_PATH", "data")  # Path to the data folder

metadata = MetaData(schema="public")  # Specify the schema

# Define the player_info table
player_info = Table(
    "player_info",
    metadata,
    Column("player_id", BigInteger, primary_key=True),
    Column("firstName", String(50)),
    Column("lastName", String(50)),
    Column("nationality", String(50)),
    Column("birthCity", String(50)),
    Column("primaryPosition", String(50)),
    Column("birthDate", Date),
    Column("birthStateProvince", String(50)),
    Column("height", Float),  # Use Float for height
    Column("height_cm", Float),  # Use Float for height_cm
    Column("weight", Float),  # Use Float for weight
    Column("shootCatches", String(10)),
)

# Create the database engine
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)

# Create the table if it does not exist
metadata.create_all(engine)
logging.info("Table created successfully.")


# Function to download a file from S3
def download_file_from_s3(bucket, key, download_path):
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


# Function to clear a directory
def clear_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logging.info(f"Cleared directory: {directory}")
    os.makedirs(directory, exist_ok=True)


# Function to convert height from "6' 1"" format to total inches
def convert_height(height_str):
    if pd.isnull(height_str):
        return None
    try:
        feet, inches = height_str.split("'")
        inches = inches.strip().replace('"', "")
        total_inches = int(feet) * 12 + int(inches)
        return total_inches
    except ValueError:
        return None


# Function to process and insert CSV data into the player_info table
def process_and_insert_csv(file_path, table):
    # Check if the file extension suggests it might be a CSV file
    if file_path.endswith(".csv") or file_path.endswith(".csv.xls"):
        # Read the file as a CSV
        df = pd.read_csv(file_path)
    else:
        # If it's a true Excel file, specify the engine
        df = pd.read_excel(file_path, engine="openpyxl")

    # Inspect the DataFrame to ensure it's being read correctly
    print("DataFrame columns:", df.columns)
    print("DataFrame head:\n", df.head())

    # Convert height from "6' 1"" format to total inches
    if "height" in df.columns:
        df["height"] = df["height"].apply(convert_height)

    # Handle NaN values by replacing them with None
    df = df.where(pd.notnull(df), None)

    # Rename columns to match the table schema
    df.rename(columns={"shootsCatches": "shootCatches"}, inplace=True)

    # Convert birthDate to datetime
    if "birthDate" in df.columns:
        df["birthDate"] = pd.to_datetime(df["birthDate"])

    # Convert the DataFrame to a list of dictionaries
    data = df.to_dict(orient="records")

    # Insert the data into the table
    session = Session()
    try:
        # Clear the existing data
        session.execute(table.delete())
        # Insert the new data
        for record in data:
            # Ensure player_id is an integer
            record["player_id"] = int(record["player_id"])
            session.execute(table.insert().values(record))
        session.commit()
        logging.info(f"Data from {file_path} inserted into the table successfully.")
    except Exception as e:
        session.rollback()
        logging.error(f"Error inserting records: {e}")
    finally:
        session.close()


# Main function to handle the workflow
def main():
    # Clear the extracted folder and recreate it
    clear_directory(local_extract_path)

    # Download the CSV file from S3
    download_file_from_s3(bucket_name, s3_file_key, local_csv_path)

    # Process and insert player_info.csv.xls
    if os.path.exists(local_csv_path):
        process_and_insert_csv(local_csv_path, player_info)
    else:
        logging.error(f"CSV file {local_csv_path} not found")

    # Clear the data folder after processing
    clear_directory(data_path)


if __name__ == "__main__":
    main()
