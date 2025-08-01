"""
August 30, 2024.

Script to merge team_record and team salaries
by season and insert data into the database.
Then delete files in team_records and
team salaries.

Eric Winiecke
"""

import os

import pandas as pd
from sqlalchemy import (
    MetaData,
)
from sqlalchemy.orm import sessionmaker

from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()

# Define metadata and tables
metadata = MetaData()

# Create tables for each season
season_year = ["2016", "2017", "2018"]

# Create tables in the database
metadata.create_all(engine)

Session = sessionmaker(bind=engine)

for season in season_year:
    try:
        # Define the paths to the CSV files for each season
        STATS_PATH = f"team_records/NHL_{int(season)}_team_stats.csv"
        SALARY_PATH = f"team_salaries/team_salary_{int(season) - 1}.csv"

        # Load the stats and team salary data
        stats_data = pd.read_csv(STATS_PATH)
        salary_data = pd.read_csv(SALARY_PATH)

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
        TABLE_NAME = f"merged_team_stats_{season}"
        merged_data.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
        print(f"Data for season {season} has been successfully inserted into the database.")

        # Delete the CSV files after successful insertion
        try:
            os.remove(STATS_PATH)
            os.remove(SALARY_PATH)
            print(f"CSV files for season {season} have been successfully deleted.")
        except OSError as e:
            print(f"Error: {e.strerror} - while deleting files for season {season}")
    except Exception as e:
        print(f"Failed to process data for season {season}. Error: {e}")
