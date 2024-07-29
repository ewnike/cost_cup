"""
July 28, 2024
main.py function that is for uploading zip files from s3,
extracting data, cleaning data, and finally inserting into
a postgres database and data tables needed fro the project.
The database and s3 buckets will be used going forward to
keep track of NHL statistics during the season.
"""

import os
import zipfile

import boto3
import pandas as pd
import psycopg2
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from psycopg2 import sql
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
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

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

# S3 Configuration
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
bucket_name = os.getenv("S3_BUCKET_NAME")
table_name_prefix = os.getenv("TABLE_NAME_PREFIX", "table_")

# List of S3 keys for zip files
S3_File_Keys = [
    "game_skater_stats.csv.zip",
    "game_plays.csv.zip",
    "game_shifts.csv.zip",
    "game.csv.zip",
]

# Initialize the S3 client with credentials
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)


def create_database_if_not_exists(db_name):
    """
    Function to create the database if it does not exist
    """
    conn = psycopg2.connect(
        dbname="postgres",  # Connect to the default 'postgres' database
        user=USER,
        password=PASSWORD,
        host=ENDPOINT,
        port=PORT,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
    exists = cur.fetchone()
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    cur.close()
    conn.close()


def download_zip_from_s3(bucket_name, s3_key, local_path):
    """
    Function to download a zip file from S3
    """
    try:
        print(f"Downloading {s3_key} from bucket {bucket_name} to {local_path}")
        s3.download_file(bucket_name, s3_key, local_path)
    except ClientError as e:
        print(f"Failed to download {s3_key} from bucket {bucket_name}: {e}")
        raise


def extract_zip(file_path, extract_to):
    """
    Function to extract a zip file
    """
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


def clean_data(df, column_mapping):
    """
    Function to clean data
    """
    df = df.where(pd.notnull(df), None)
    for column, dtype in df.dtypes.items():
        if dtype == "object":
            df[column] = df[column].apply(
                lambda x: str(x).strip()[:255] if isinstance(x, str) else x
            )
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
            df[csv_column] = (
                pd.to_numeric(df[csv_column], downcast="integer", errors="coerce")
                .fillna(pd.NA)
                .astype(pd.Int64Dtype())
            )
        elif db_column in ["x", "y"]:
            df[csv_column] = pd.to_numeric(df[csv_column], errors="coerce")
        elif db_column == "dateTime":
            df[csv_column] = pd.to_datetime(df[csv_column], errors="coerce")
    df = df.drop_duplicates(ignore_index=True)
    for column in df.select_dtypes(include=["Int64"]).columns:
        if df[column].max() > 2147483647 or df[column].min() < -2147483648:
            print(f"Rows with out of range values in column '{column}':")
            print(df[(df[column] > 2147483647) | (df[column] < -2147483648)])
    return df


def create_table(metadata, engine, table_definitions):
    """
    Function to create tables
    """
    for table_def in table_definitions:
        table_def.create(engine)


def insert_data(df, table, session):
    """
    Function to insert data with progress bar
    """
    data = df.to_dict(orient="records")
    try:
        with tqdm(total=len(data), desc=f"Inserting data into {table.name}") as pbar:
            for record in data:
                session.execute(table.insert().values(**record))
                session.commit()
                pbar.update(1)
        print(f"Data inserted successfully into {table.name}")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error inserting data into {table.name}: {e}")


def process_and_insert_csv(csv_file_path, table, column_mapping, session):
    """
    Function to process and insert CSV data
    """
    try:
        print(f"Processing {csv_file_path} for table {table.name}")
        df = pd.read_csv(csv_file_path)
        print(f"DataFrame for {table.name} loaded with {len(df)} records")
        print("Column Datatypes:")
        print(df.dtypes)
        print("Sample data:")
        print(df.head())
        df = clean_data(df, column_mapping)
        print(f"DataFrame for {table.name} cleaned with {len(df)} records")
        print("Cleaned sample data:")
        print(df.head())
        insert_data(df, table, session)
    except FileNotFoundError as e:
        print(f"File not found: {csv_file_path} - {e}")


def main():
    """
    put it all together.
    """
    # Create the database if it does not exist
    create_database_if_not_exists(DATABASE)

    # Get database engine
    engine = create_engine(connection_string)
    metadata = MetaData()

    # Define table schemas
    tables = {
        "game": Table(
            "game",
            metadata,
            Column("game_id", BigInteger, primary_key=True),
            Column("season", Integer),
            Column("type", String(25)),
            Column("date_time_GMT", DateTime(timezone=True)),
            Column("away_team_id", Integer),
            Column("home_team_id", Integer),
            Column("away_goals", Integer),
            Column("home_goals", Integer),
            Column("outcome", String(50)),
            Column("home_rink_side_start", String(50)),
            Column("venue", String(50)),
            Column("venue_link", String(50)),
            Column("venue_time_zone_id", String(50)),
            Column("venue_time_zone_offset", String(25)),
            Column("venue_time_zone_tz", String(25)),
        ),
        "game_plays": Table(
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
        ),
        "game_shifts": Table(
            "game_shifts",
            metadata,
            Column("game_id", BigInteger),
            Column("player_id", BigInteger),
            Column("period", Integer),
            Column("shift_start", Integer),
            Column("shift_end", Integer),
        ),
        "game_skater_stats": Table(
            "game_skater_stats",
            metadata,
            Column("game_id", BigInteger),
            Column("player_id", BigInteger),
            Column("team_id", Integer),
            Column("timeOnIce", Integer),
            Column("assists", Integer),
            Column("goals", Integer),
            Column("shots", Integer),
            Column("hits", Integer),
            Column("powerPlayGoals", Integer),
            Column("powerPlayAssists", Integer),
            Column("penaltyMinutes", Integer),
            Column("faceOffWins", Integer),
            Column("faceoffTaken", Integer),
            Column("takeaways", Integer),
            Column("giveaways", Integer),
            Column("shortHandedGoals", Integer),
            Column("shortHandedAssists", Integer),
            Column("blocked", Integer),
            Column("plusMinus", Integer),
            Column("evenTimeOnIce", Integer),
            Column("shortHandedTimeOnIce", Integer),
            Column("powerPlayTimeOnIce", Integer),
        ),
    }

    # Create tables if they do not exist
    create_table(metadata, engine, tables.values())

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Drop existing tables if needed
    with engine.connect() as connection:
        for table_name in tables.keys():
            connection.execute(text(f"DROP TABLE IF EXISTS public.{table_name};"))

    # Download and extract zip files from S3
    local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/download")
    os.makedirs(local_extract_path, exist_ok=True)

    for s3_key in S3_File_Keys:
        local_zip_path = os.path.join(local_extract_path, s3_key)
        download_zip_from_s3(bucket_name, s3_key, local_zip_path)
        extract_zip(local_zip_path, local_extract_path)

    # Define CSV file paths and column mappings
    csv_files = {
        "game": {
            "file_path": os.path.join(local_extract_path, "game.csv"),
            "column_mapping": {
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
            },
        },
        "game_plays": {
            "file_path": os.path.join(local_extract_path, "game_plays.csv"),
            "column_mapping": {
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
            },
        },
        "game_shifts": {
            "file_path": os.path.join(local_extract_path, "game_shifts.csv"),
            "column_mapping": {
                "game_id": "game_id",
                "player_id": "player_id",
                "period": "period",
                "shift_start": "shift_start",
                "shift_end": "shift_end",
            },
        },
        "game_skater_stats": {
            "file_path": os.path.join(local_extract_path, "game_skater_stats.csv"),
            "column_mapping": {
                "game_id": "game_id",
                "player_id": "player_id",
                "team_id": "team_id",
                "timeOnIce": "timeOnIce",
                "assists": "assists",
                "goals": "goals",
                "shots": "shots",
                "hits": "hits",
                "powerPlayGoals": "powerPlayGoals",
                "powerPlayAssists": "powerPlayAssists",
                "penaltyMinutes": "penaltyMinutes",
                "faceOffWins": "faceOffWins",
                "faceoffTaken": "faceoffTaken",
                "takeaways": "takeaways",
                "giveaways": "giveaways",
                "shortHandedGoals": "shortHandedGoals",
                "shortHandedAssists": "shortHandedAssists",
                "blocked": "blocked",
                "plusMinus": "plusMinus",
                "evenTimeOnIce": "evenTimeOnIce",
                "shortHandedTimeOnIce": "shortHandedTimeOnIce",
                "powerPlayTimeOnIce": "powerPlayTimeOnIce",
            },
        },
    }

    # Process and insert each CSV file
    for table_name, info in csv_files.items():
        if os.path.exists(info["file_path"]):
            process_and_insert_csv(
                info["file_path"], tables[table_name], info["column_mapping"], session
            )
        else:
            print(f"CSV file {info['file_path']} not found")


if __name__ == "__main__":
    main()
