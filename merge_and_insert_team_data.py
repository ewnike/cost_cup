"""
August 30, 2024
Script to merge team_record and team salaries
by season and insert data into the database.
Then delete files in team_records and
team salaries.
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    MetaData,
    create_engine,
)
from sqlalchemy.orm import sessionmaker

# Load environment variables from the .env file
load_dotenv()

# Retrieve database connection parameters from environment variables
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5432))  # Provide default value if not set
DATABASE = os.getenv("DATABASE")

# Create the connection string
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)
engine = create_engine(connection_string)

# Define metadata and tables
metadata = MetaData()

# Create tables for each season
seasons = ["2016", "2017", "2018"]
# tables = {season: create_team_data_table(f"team_data_{season}") for season in seasons}

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)

for season in seasons:
    try:
        # Define the paths to the CSV files for each season
        stats_path = f"team_records/NHL_{int(season)}_team_stats.csv"
        salary_path = f"team_salaries/team_salary_{int(season) - 1}.csv"

        # Load the stats and team salary data
        stats_data = pd.read_csv(stats_path)
        salary_data = pd.read_csv(salary_path)

        # Merge on different column names
        merged_data = pd.merge(
            stats_data,
            salary_data,
            left_on="Abbreviation",
            right_on="Team",
            how="inner",
        )

        # Drop the redundant 'Team_y' column from the merged DataFrame
        merged_data.drop(columns=["Team_y"], inplace=True)

        # Display the merged data to verify
        print(f"Merged data for season {season}:\n", merged_data.head())

        # Insert the merged data into a new table in the database
        table_name = f"merged_team_stats_{season}"
        merged_data.to_sql(table_name, engine, if_exists="replace", index=False)
        print(
            f"Data for season {season} has been successfully inserted into the database."
        )

        # Delete the CSV files after successful insertion
        try:
            os.remove(stats_path)
            os.remove(salary_path)
            print(f"CSV files for season {season} have been successfully deleted.")
        except OSError as e:
            print(f"Error: {e.strerror} - while deleting files for season {season}")
    except Exception as e:
        print(f"Failed to process data for season {season}. Error: {e}")
