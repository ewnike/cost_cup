import altair as alt
import pandas as pd

data = pd.read_csv(
    r"/Users/ericwiniecke/Documents/github/cost_cup/wins_and_cfpercent/team_wins_and_cfpercent_2018.csv"
)

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
    "Chart saved as 'team_payroll_points_chart.html'. Open this file in your browser to view the chart."
)
