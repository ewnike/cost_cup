import os

import altair as alt
import altair_viewer
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

# Ensure numeric types for Altair
df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
df["CF_Percent"] = pd.to_numeric(df["CF_Percent"], errors="coerce")
df = df.dropna(subset=["capHit", "CF_Percent"])
print(df)
# Create scatter plots for each season using Altair
charts = []

for season in seasons:
    df_season = df[df["season"] == season]

    # Avoid empty DataFrames
    if df_season.empty:
        print(f"No data available for season {season}")
        continue

    scatter_plot = (
        alt.Chart(df_season)
        .mark_circle(size=60)
        .encode(
            x=alt.X("CF_Percent", title="Corsi For Percentage (CF%)"),
            y=alt.Y("capHit", title="Cap Hit ($)"),
            tooltip=["CF_Percent", "capHit"],
            opacity=alt.value(0.6),
        )
        .properties(title=f"Season {season}", width=250, height=300)
    )

    charts.append(scatter_plot)

# # Combine the charts into a horizontal layout
combined_chart = alt.concat(*charts, columns=3)
print("Howdy Doody")
# combined_chart.show()
alt.renderers.enable('png')
combined_chart

# # Save the chart as an HTML file and open it
combined_chart.save("combined_chart.html")
# altair_viewer.show(combined_chart)
