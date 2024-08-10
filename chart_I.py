import os

import matplotlib.pyplot as plt
import numpy as np
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
    SELECT "capHit"
    FROM public.aggregated_corsi_{season}
    """
    season_df = pd.read_sql(query, engine)
    season_df["season"] = season  # Add the season column manually in Python
    df = pd.concat([df, season_df], ignore_index=True)

# Convert `capHit` to numeric if it's not already
df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")

# Drop any null values that might exist after conversion
df = df.dropna(subset=["capHit"])

# Define the number of bins and bin width
bin_width = 300000  # $300,000
bins = np.arange(575000, df["capHit"].max() + bin_width, bin_width)

# Create subplots
fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6), sharey=True)

# Loop through each season and create a histogram in its own subplot
for ax, season in zip(axes, seasons):
    # Filter data for the specific season
    df_season = df[df["season"] == season]

    # Calculate the histogram manually to get the counts
    counts, bin_edges = np.histogram(df_season["capHit"], bins=bins)

    # Create bin labels for the x-axis
    bin_labels = [
        f"${edge/1000:,.0f}k-${(edge + bin_width)/1000:,.0f}k"
        for edge in bin_edges[:-1]
    ]

    # Plot the histogram
    ax.bar(bin_labels, counts, width=0.7, edgecolor="black")

    # Set title and labels
    ax.set_title(f"Season {season}")
    ax.set_xlabel("Salary Range")
    ax.set_xticks(np.arange(len(bin_labels)))
    ax.set_xticklabels(bin_labels, rotation=45, ha="right")

# Set the y-label on the leftmost subplot
axes[0].set_ylabel("Number of Players")

# Adjust layout
plt.tight_layout()

# Show the plot
plt.show()
