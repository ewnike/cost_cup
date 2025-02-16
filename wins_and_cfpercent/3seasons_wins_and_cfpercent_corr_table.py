"""
Saturday, February 1st, 2025.

Code to calculate the wins and CF%
for teams by season.

Eric Winiecke.
"""

import os
import sys

import pandas as pd
from sqlalchemy import text

# Dynamically add the parent directory (cost_cup/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_utils import get_db_engine, get_metadata

# Get database engine and metadata
engine = get_db_engine()
metadata = get_metadata()

# Define seasons and corresponding years
seasons = ["20152016", "20162017", "20172018"]
years = ["2016", "2017", "2018"]

# Initialize a list to store DataFrames
df_list = []

# Loop through each season and year to fetch data
for season, year in zip(seasons, years):
    sql_query = f"""
    SELECT
        mt."Abbreviation",
        mt."PTS",
        mt."Total_Payroll",
        ROUND(AVG(ac."CF%")::numeric, 3) AS avg_cf_percent
    FROM public.team_event_totals_season_{season} ac
    JOIN public.merged_team_stats_{year} mt ON ac.team_id = mt."Team_ID"
    GROUP BY ac.team_id, mt."Abbreviation", mt."PTS", mt."Total_Payroll"
    ORDER BY mt."PTS" DESC;
    """

    # Execute the query and fetch results into a Pandas DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(sql_query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Append DataFrame to the list
        df_list.append(df)

# Concatenate all DataFrames into one
df = pd.concat(df_list, axis=0, ignore_index=True)

# Convert "Total_Payroll" to integer after removing dollar signs and commas
df["Total_Payroll"] = (
    df["Total_Payroll"].astype(str).str.replace(r"[\$,]", "", regex=True).astype(int)
)

# Compute correlation matrix
correlation_table = df.drop(columns=["Abbreviation"]).corr()

# Print results
print("Correlation Table:")
print(correlation_table)
