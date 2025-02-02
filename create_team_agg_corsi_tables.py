"""
November 11, 2024
Script to create 'team_agg_corsi' tables with expanded columns for Corsi stats.
Eric Winiecke
"""

import logging

from sqlalchemy import BIGINT, FLOAT, Column, Table
from sqlalchemy.exc import SQLAlchemyError

from db_utils import get_db_engine, get_metadata

# Initialize engine and metadata
engine = get_db_engine()
metadata = get_metadata()


# Helper function to define the table schema
def create_team_agg_corsi_table(table_name):
    return Table(
        table_name,
        metadata,
        Column("team_id", BIGINT, primary_key=True),
        Column("total_goals", BIGINT),
        Column("total_shots", BIGINT),
        Column("total_missed_shots", BIGINT),
        Column("total_blocked_shots_against", BIGINT),
        Column("total_goals_against", BIGINT),
        Column("total_shots_against", BIGINT),
        Column("total_missed_shots_against", BIGINT),
        Column("total_blocked_shots_for", BIGINT),
        Column("corsi_for", BIGINT),  # CF (Corsi For)
        Column("corsi_against", BIGINT),  # CA (Corsi Against)
        Column("corsi", BIGINT),  # C (Corsi Differential)
        Column("cf_percent", FLOAT),  # CF% (Corsi For Percentage)
    )


# Define the seasons and table names
seasons = ["20152016", "20162017", "20172018"]
tables = {
    season: create_team_agg_corsi_table(f"team_agg_corsi_{season}")
    for season in seasons
}

# Create the tables in the database
try:
    metadata.create_all(engine)
    logging.info("Tables with expanded Corsi stats created successfully.")
except SQLAlchemyError as e:
    logging.error(f"Error creating tables: {e}")
