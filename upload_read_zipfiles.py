"""
July 27, 2024
Code to upload NHL game data from s3 bucket,
unzip file, read file, create database table,
and insert data into those tables. Finally remove the
zip and csv file.
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
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

# Retrieve database connection parameters from environment variables
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5432))  # Provide default value if not set
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
    "game_plays_players.csv.zip",
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


# Function to create the database if it does not exist
def create_database_if_not_exists(db_name):
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


# Function to download a zip file from S3
def download_zip_from_s3(bucket_name, s3_key, local_path):
    try:
        print(f"Downloading {s3_key} from bucket {bucket_name} to {local_path}")
        s3.download_file(bucket_name, s3_key, local_path)
    except ClientError as e:
        print(f"Failed to download {s3_key} from bucket {bucket_name}: {e}")
        raise


# Function to extract a zip file
def extract_zip(file_path, extract_to):
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


# Function to insert data from CSV into PostgreSQL
def insert_data_from_csv(session, table, csv_file_path, column_mapping):
    try:
        df = pd.read_csv(csv_file_path)
        for _, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc=f"Inserting {os.path.basename(csv_file_path)}",
        ):
            data = {
                column: row[csv_column] for column, csv_column in column_mapping.items()
            }
            session.execute(table.insert().values(**data))
        session.commit()
        print(f"Data inserted successfully into {table.name}")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error inserting data into {table.name}: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {csv_file_path} - {e}")


# Define table schemas
metadata = MetaData()

game = Table(
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
    Column("venue_time_zone_id", Integer),
    Column("venue_time_zone_offset", String(25)),
    Column("venue_time_zone_tz", String(25)),
)

game_plays = Table(
    "game_plays",
    metadata,
    Column("play_id", String(20)),
    Column("game_id", BigInteger),
    Column("team_id_for", Float, nullable=True),
    Column("team_id_against", Float, nullable=True),
    Column("event", String(50)),
    Column("secondaryType", String(50)),
    Column("x", Float, nullable=True),
    Column("y", Float, nullable=True),
    Column("period", Integer),
    Column("periodType", String(50)),
    Column("periodTime", Float),  # Changed from BigInteger to Float
    Column("periodTimeRemaining", Float, nullable=True),
    Column("dateTime", DateTime(timezone=False)),
    Column("goals_away", Integer, nullable=True),
    Column("goals_home", Integer, nullable=True),
    Column("description", String(50)),
    Column("st_x", Integer, nullable=True),
    Column("st_y", Integer, nullable=True),
)

game_shifts = Table(
    "game_shifts",
    metadata,
    Column("game_id", BigInteger),
    Column("player_id", BigInteger),
    Column("period", Integer),
    Column("shift_start", BigInteger),
    Column("shift_end", BigInteger),
)

game_skater_stats = Table(
    "game_skater_stats",
    metadata,
    Column("game_id", BigInteger),
    Column("player_id", BigInteger),
    Column("team_id", Integer),
    Column("timeOnIce", BigInteger),
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
    Column("evenTimeOnIce", BigInteger),
    Column("shortHandedTimeOnIce", BigInteger),
    Column("powerPlayTimeOnIce", BigInteger),
)

tables = [game, game_plays, game_shifts, game_skater_stats]

# Initialize database
create_database_if_not_exists(DATABASE)

# Create the database engine
engine = create_engine(connection_string)

# Drop tables if they exist
tables_to_drop = ["game", "game_plays", "game_shifts", "game_skater_stats"]

with engine.connect() as connection:
    for table in tables_to_drop:
        connection.execute(f"DROP TABLE IF EXISTS {table};")

# Create tables with the updated schema
metadata.create_all(engine)

Session = sessionmaker(bind=engine)

# Define the column mappings for each CSV file
csv_files_and_mappings = [
    (
        "game.csv",
        game,
        {
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
    ),
    (
        "game_plays.csv",
        game_plays,
        {
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
        },
    ),
    (
        "game_shifts.csv",
        game_shifts,
        {
            "game_id": "game_id",
            "player_id": "player_id",
            "period": "period",
            "shift_start": "shift_start",
            "shift_end": "shift_end",
        },
    ),
    (
        "game_skater_stats.csv",
        game_skater_stats,
        {
            "player_id": "player_id",
            "game_id": "game_id",
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
    ),
]


# Main function to handle the workflow
def main():
    # Get the current script directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    local_dir = os.path.join(project_root, "data", "downloads")
    extract_to_path = os.path.join(project_root, "data", "extracted")

    # Ensure the local directory exists
    os.makedirs(local_dir, exist_ok=True)
    os.makedirs(extract_to_path, exist_ok=True)

    for s3_key in S3_File_Keys:
        local_zip_path = os.path.join(local_dir, os.path.basename(s3_key))

        # Download the zip file from S3
        download_zip_from_s3(bucket_name, s3_key, local_zip_path)

        # Extract the zip file
        extract_zip(local_zip_path, extract_to_path)

        # Process each CSV file in the extracted directory
        for csv_file, table, column_mapping in csv_files_and_mappings:
            csv_file_path = os.path.join(extract_to_path, csv_file)
            with Session() as session:
                insert_data_from_csv(session, table, csv_file_path, column_mapping)

        # Clean up the zip file
        os.remove(local_zip_path)


if __name__ == "__main__":
    main()
