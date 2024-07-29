"""
July 28, 2024
This is a db_utils file that holds information
to connect to the database. It also has the connection
information for AWS to retrieve zip files
from an S3 bucket.
"""

import os
import zipfile

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


# load environment variables
def load_env_variables():
    """
    load env variables.
    """
    load_dotenv()
    return {
        "DATABASE_TYPE": os.getenv("DATABASE_TYPE"),
        "DBAPI": os.getenv("DBAPI"),
        "ENDPOINT": os.getenv("ENDPOINT"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "PORT": int(os.getenv("PORT", 5432)),
        "DATABASE": os.getenv("DATABASE", "hockey_stats"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_BUCKET_NAME": os.getenv("AWS_BUCKET_NAME"),
        "LOCAL_EXTRACT_PATH": os.getenv("LOCAL_EXTRACT_PATH", "data/extracted"),
    }

"""Build connection string"""
def get_db_engine(env_vars):
    """
    Create a SQLAlchemy engine using the provided environment variables.

    Args:
        env_vars (dict): A dictionary containing the environment variables for the database connection.

    Returns:
        sqlalchemy.engine.Engine: A SQLAlchemy engine object.
    """
    connection_string = (
        f"{env_vars['DATABASE_TYPE']}+{env_vars['DBAPI']}://"
        f"{env_vars['USER']}:{env_vars['PASSWORD']}@"
        f"{env_vars['ENDPOINT']}:{env_vars['PORT']}/"
        f"{env_vars['DATABASE']}"
    )
    return create_engine(connection_string)


def clean_data(df, column_mapping):
    """
    code for cleaning all 4 data files
    needed for the project.
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
    function to create data tables.
    """
    for table_def in table_definitions:
        table_def.create(engine)


def insert_data(df, table, session):
    """
    function to insert_data into the data tables
    """
    data = df.to_dict(orient="records")
    try:
        session.execute(table.insert(), data)
        session.commit()
        print(f"Data inserted successfully into {table.name}")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error inserting data into {table.name}: {e}")


def process_and_insert_csv(csv_file_path, table, column_mapping, session):
    """
    Messages to follow process and make sure all is working
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


def download_and_extract_zip_from_s3(
    bucket_name, s3_key, local_extract_path, aws_access_key_id, aws_secret_access_key
):
    """
    function to connect to S3 bucket, download, and extract data
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    local_zip_path = os.path.join(local_extract_path, os.path.basename(s3_key))
    s3.download_file(bucket_name, s3_key, local_zip_path)
    with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
        zip_ref.extractall(local_extract_path)
    os.remove(local_zip_path)  # Clean up the zip file after extraction
