import os
import pandas as pd
from db_utils import get_db_engine

# Initialize database connection
engine = get_db_engine()

# Define the seasons to process
seasons = ["20152016", "20162017", "20172018"]

# Create an empty list to store the data from each season
season_dataframes = []

# Loop through each season and query the data
for season in seasons:
    query = f"""
    SELECT 
        mts."Abbreviation", 
        mts."PTS", 
        mts."Total_Payroll", 
        ROUND(CAST(tets."CF%" * 100 AS numeric), 2) AS "avg_cf_percent",
        '{season}' AS "Season"
    FROM public.merged_team_stats_{season[:4]} mts
    JOIN public.team_event_totals_season_{season} tets
        ON mts."Team_ID" = tets."team_id"
    """
    try:
        # Fetch the data for the current season
        df = pd.read_sql(query, engine)

        # Ensure the 'Total_Payroll' column is numeric
        df["Total_Payroll"] = pd.to_numeric(df["Total_Payroll"].replace("[\$,]", "", regex=True), errors="coerce")

        # Append the dataframe to the list
        season_dataframes.append(df)
    except Exception as e:
        print(f"An error occurred for season {season}: {e}")

# Combine all the season data into one DataFrame
all_season_data = pd.concat(season_dataframes, ignore_index=True)

# Save the combined data to a CSV file
output_file = "combined_season_data.csv"
all_season_data.to_csv(output_file, index=False)
print(f"Combined data saved to {output_file}")
