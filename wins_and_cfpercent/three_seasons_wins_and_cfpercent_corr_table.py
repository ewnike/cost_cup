"""
Saturday, February 1st, 2025.

Code to calculate the wins and CF%
for teams by season. Run using python -m ....

Eric Winiecke.
"""

import pandas as pd
from sqlalchemy import text

from constants import SEASONS_LEGACY as SEASONS
from db_utils import get_db_engine, get_metadata

# Get database engine and metadata
engine = get_db_engine()
metadata = get_metadata()

# Define seasons and corresponding years
# SEASONS = ["20152016", "20162017", "20172018"]

years = ["2016", "2017", "2018"]

# Initialize a list to store DataFrames
df_list = []

# Loop through each season and year to fetch data
for season, year in zip(SEASONS, years):
    SQL_QUERY = f"""
    SELECT
        mt.abbr,
        mt.pts,
        mt.total_cap AS total_payroll,
        ROUND(
        100.0 * SUM(ac.total_shots + ac.total_missed_shots + ac.total_blocked_shots_for)
        / NULLIF(
            SUM(ac.total_shots + ac.total_missed_shots + ac.total_blocked_shots_for)
            + SUM(ac.total_shots_against + ac.total_missed_shots_against + ac.total_blocked_shots_against),
            0
        )
        , 3) AS cf_percent
    FROM derived.team_event_totals_games_{season} ac
    JOIN mart.team_summary_{season} mt
    ON ac.team_id = mt.team_id
    GROUP BY mt.team_id, mt.abbr, mt.pts, mt.total_cap
    ORDER BY mt.pts DESC;
    """

    # Execute the query and fetch results into a Pandas DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(SQL_QUERY))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Append DataFrame to the list
        df_list.append(df)

# Concatenate all DataFrames into one
df = pd.concat(df_list, axis=0, ignore_index=True)

# NEW: ensure payroll is numeric (since mt.total_cap is already numeric)
df["total_payroll"] = pd.to_numeric(df["total_payroll"], errors="coerce")

# Compute correlation matrix
correlation_table = df.drop(columns=["abbr"]).corr(numeric_only=True)

# Print results
print("Correlation Table:")
print(correlation_table)
