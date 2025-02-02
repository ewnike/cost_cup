import altair as alt
import pandas as pd
# from sqlalchemy import create_engine
from db_utils import get_db_engine, get_metadata

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()


# Define a function to fetch data for a specific season
# def fetch_season_data(season_stats_table, season_corsi_table, season_label):
#     query = f"""
#     SELECT mts."Abbreviation", mts."PTS", mts."Total_Payroll", 
#            ROUND(CAST(avg_ac."avg_cf_percent" AS numeric), 2) AS "avg_cf_percent",
#            '{season_label}' AS "Season"
#     FROM {season_stats_table} mts
#     JOIN (
#         SELECT "team_id", AVG("CF_Percent") AS "avg_cf_percent"
#         FROM {season_corsi_table}
#         GROUP BY "team_id"
#     ) avg_ac
#     ON mts."Team_ID" = avg_ac."team_id";
#     """
#     return pd.read_sql(query, engine)


# # Fetch data for each season
# data_2016 = fetch_season_data(
#     "merged_team_stats_2016", "team_event_totals_season_20152016", "2015-2016"
# )
# data_2017 = fetch_season_data(
#     "merged_team_stats_2017", "team_event_totals_season_20162017", "2016-2017"
# )
# data_2018 = fetch_season_data(
#     "merged_team_stats_2018", "team_event_totals_season_20172018", "2017-2018"
# )
# Define a function to fetch data for a specific season
# def fetch_season_data(season_stats_table, season_corsi_table, season_label):
#     query = f"""
#     SELECT
#         mts."Abbreviation",
#         mts."PTS",
#         mts."Total_Payroll",
#         ROUND(CAST(tets."CF%" * 100 AS numeric), 2) AS "avg_cf_percent",
#         '{season_label}' AS "Season"
#     FROM {season_stats_table} mts
#     JOIN {season_corsi_table} tets
#         ON mts."Team_ID" = tets."team_id"
#     """
#     return pd.read_sql(query, engine)

def fetch_season_data(season_stats_table, season_corsi_table, season_label):
    query = f"""
    SELECT 
        mts."Abbreviation", 
        mts."PTS", 
        mts."Total_Payroll", 
        ROUND(CAST(tets."CF%" * 100 AS numeric), 4) AS "avg_cf_percent",
        '{season_label}' AS "Season"
    FROM {season_stats_table} mts
    JOIN {season_corsi_table} tets
        ON mts."Team_ID" = tets."team_id";
    """
    return pd.read_sql(query, engine)


# Fetch data for each season
data_2016 = fetch_season_data(
    "merged_team_stats_2016", "team_event_totals_season_20152016", "2015-2016"
)
data_2017 = fetch_season_data(
    "merged_team_stats_2017", "team_event_totals_season_20162017", "2016-2017"
)
data_2018 = fetch_season_data(
    "merged_team_stats_2018", "team_event_totals_season_20172018", "2017-2018"
)
print(data_2016.head())
print(data_2017.head())
print(data_2018.head())

# Concatenate the data from all seasons
data = pd.concat([data_2016, data_2017, data_2018])

# Clean and convert the Total_Payroll column to numerical values (assuming it's stored as a string with $ symbols)
# data["Total_Payroll"] = data["Total_Payroll"].replace("[\$,]", "", regex=True).astype(float)

# Clean and convert the Total_Payroll column to numerical values (assuming it's stored as a string with $ symbols)
# data["Total_Payroll"] = data["Total_Payroll"].replace(r"[\$,]", "", regex=True).astype(float)

# data["Total_Payroll"] = (
#     data["Total_Payroll"].replace("[\\$,]", "", regex=True).astype(float)
# )

# Divide teams into salary quantiles (e.g., 4 quantiles: Q1, Q2, Q3, Q4)
data["Salary_Quantile"] = pd.qcut(
    data["Total_Payroll"], 4, labels=["Q1", "Q2", "Q3", "Q4"]
)

# Find the lowest PTS value across all seasons
min_pts = data["PTS"].min()

# Create a scale for avg_cf_percent
cf_percent_scale = alt.Scale(domain=[45, 56])

# Create the chart with color for salary quantiles and team abbreviations as text labels
chart = (
    alt.Chart(data)
    .mark_text(size=16, dx=0, dy=-5, fontWeight="bold")  # Use text instead of points
    .encode(
        x=alt.X(
            "PTS:Q",
            title="Points (PTS)",
            scale=alt.Scale(domain=[min_pts, data["PTS"].max()]),
        ),  # Truncate at the lowest PTS
        y=alt.Y(
            "avg_cf_percent:Q",
            scale=cf_percent_scale,
            title="Average Corsi For Percentage (CF%)",
            axis=alt.Axis(titlePadding=30, titleFontSize=14)
        ),
        color=alt.Color(
            "Salary_Quantile:N", title="Salary Quantile"
        ),  # Color based on salary quantile
        text=alt.Text("Abbreviation:N"),  # Use the team abbreviation as text
        tooltip=[
            "Abbreviation",
            "PTS",
            "Total_Payroll",
            "avg_cf_percent",
            "Salary_Quantile",
            "Season",
        ],  # Add tooltip for more info
    )
    .properties(
        # title="",
        width=450,
        height=425,
    )
    .facet(  # Create separate charts for each season
        column=alt.Column(
            "Season:N",
            title="Team Points vs Average Corsi For Percentage (Seasons: 2016-2018)",
            header=alt.Header(
                labelFontSize=14,  # Set the text size
                labelFont="Arial",  # Set the font
                labelFontWeight="bold",  # Make the text bold
                titleFontSize=16,  # Set the title size
                titleFont="Arial",  # Set the title font
                titleFontWeight="bold",  # Make the title bold)
            ),
        )
    )
)

# Display the chart
print(chart.to_json())
chart