import os

import matplotlib.pyplot as plt

# import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

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
engine = create_engine(connection_string)

# Define the seasons to query
seasons = ["20152016", "20162017", "20172018"]

# Initialize an empty DataFrame to collect all the data
df = pd.DataFrame()

# Loop through each season and query the corresponding table
for season in seasons:
    query = f"""
    SELECT "capHit", "CF_Percent"
    FROM public.aggregated_corsi_{season}
    """
    season_df = pd.read_sql(query, engine)
    season_df["season"] = season  # Add the season column manually in Python
    df = pd.concat([df, season_df], ignore_index=True)

# Convert `capHit` and `CF_Percent` to numeric if they aren't already
df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
df["CF_Percent"] = pd.to_numeric(df["CF_Percent"], errors="coerce")

# Drop any null values that might exist after conversion
df = df.dropna(subset=["capHit", "CF_Percent"])

# Create subplots for each season
fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6), sharey=True, sharex=True)

# Loop through each season and create a scatter plot in its own subplot
for ax, season in zip(axes, seasons):
    # Filter data for the specific season
    df_season = df[df["season"] == season]

    # Create the scatter plot with CF_Percent on x-axis and capHit on y-axis
    ax.scatter(
        df_season["CF_Percent"],
        df_season["capHit"],
        alpha=0.6,
        edgecolors="w",
        linewidth=0.5,
    )

    # Set title and labels
    ax.set_title(f"Season {season}")
    ax.set_xlabel("Corsi For Percentage (CF%)")
    ax.set_ylabel("Cap Hit ($)")
    ax.grid(True)

# Set the y-label only on the first subplot (since sharey=True)
axes[0].set_ylabel("Cap Hit ($)")

# Adjust layout
plt.tight_layout()

# Show the plot
plt.show()
