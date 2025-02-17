"""
Visualization of Team Payroll vs Points, Color-Coded by CF%.

This script reads a CSV file containing NHL team statistics and generates an interactive
scatter plot using Altair. The plot displays team payroll vs. points, with color encoding
based on CF% (Corsi For percentage). Team abbreviations are added as labels for clarity.

The chart is saved as an HTML file for viewing in a web browser.

Author: Eric Winiecke
Date: February 2025
"""

import os

import altair as alt
import pandas as pd

# Define the dynamic file path to allow cross-system execution
base_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(
    base_dir, "wins_and_cfpercent", "team_wins_and_cfpercent_2018.csv"
)

# Load data
data = pd.read_csv(file_path)

# Scatter plot: Points vs Payroll, color-coded by CF%
scatter = (
    alt.Chart(data)
    .mark_circle(size=100)
    .encode(
        x="payroll:Q",
        y="pts:Q",
        color="cf_percent:Q",
        tooltip=["Abbreviation", "PTS", "Total_Payroll", "avg_cf_percent"],
    )
    .properties(title="Team Payroll vs Points, Color Coded by CF%")
)

# Add team labels
text = scatter.mark_text(align="left", baseline="middle", dx=7).encode(
    text="Abbreviation"
)


# Combine the scatter plot and text labels
chart = scatter + text

# Save the chart as an HTML file
chart.save("team_payroll_points_chart.html")

print(
    "Chart saved as 'team_payroll_points_chart.html'. "
    "Open this file in your browser to view the chart."
)
