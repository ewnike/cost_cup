"""
Saturday, February 1st, 2025
Code to calculate the wins and cf%
for teams by season.

Eric Winiecke
"""


import os
import pandas as pd
from sqlalchemy import text

from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()

# Directory to store CSV files
OUTPUT_DIR = "wins_and_cfpercent"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define seasons and corresponding years
seasons = ["20152016", "20162017", "20172018"]
years = ["2016", "2017", "2018"]

# Initialize a dictionary to store DataFrames for each season
dfs = {}

# Loop through each season and year
for season, year in zip(seasons, years):
    # Define the SQL query for each season and year
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

    # Execute the query and fetch the results into a Pandas DataFrame
    with engine.connect() as connection:
        result = connection.execute(text(sql_query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Store the DataFrame for each season in the dictionary
        dfs[season] = df

        # Save each DataFrame to a CSV file in the output directory
        output_file = os.path.join(OUTPUT_DIR, f"team_wins_and_cfpercent_{year}.csv")
        df.to_csv(output_file, index=False)
        print(f"Data for {season} saved to '{output_file}'.")

# Access individual DataFrames by season
df_20152016 = dfs["20152016"]
df_20162017 = dfs["20162017"]
df_20172018 = dfs["20172018"]

    # Save DataFrame to a CSV file
output_file = os.path.join(OUTPUT_DIR, f"team_wins_and_cfpercent_{year}.csv")
df.to_csv(output_file, index=False)

# Display the DataFrames separately
print("Data for 20152016 Season:")
print(df_20152016)

print("\nData for 20162017 Season:")
print(df_20162017)

print("\nData for 20172018 Season:")
print(df_20172018)
