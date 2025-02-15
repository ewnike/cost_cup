"""Module to analyze CF% by salary quantile."""


import altair as alt
import pandas as pd

from db_utils import get_db_engine

# Initialize database connection
engine = get_db_engine()

# Define the seasons you want to visualize
seasons = ["20152016", "20162017", "20172018"]

# Create an empty list to store the data from each season
dataframes = []

# Loop through each season and query the data
for season in seasons:
    query = f"""
    SELECT "player_id", "capHit", "CF_Percent"
    FROM public.aggregated_corsi_{season}
    """
    # Fetch the data from the database for the current season
    df = pd.read_sql(query, engine)

    # Ensure the 'capHit' and 'CF_Percent' columns are numeric
    df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
    df["CF_Percent"] = pd.to_numeric(df["CF_Percent"], errors="coerce")

    # Remove any rows with missing data in either 'capHit' or 'CF_Percent'
    df = df.dropna(subset=["capHit", "CF_Percent"])

    # Add a column for the season
    df["season"] = season

    # Append the data to the list
    dataframes.append(df)

# Combine all the dataframes into one DataFrame
df = pd.concat(dataframes)

# Compute salary quantiles ('Q1' to 'Q4') across all seasons based on 'capHit'
df["Salary_Quantile"] = pd.qcut(df["capHit"], 4, labels=["Q1", "Q2", "Q3", "Q4"])

# Compute the salary range (min and max) for each quantile across seasons
salary_ranges = (
    df.groupby("Salary_Quantile", observed=False)["capHit"]
    .agg(["min", "max"])
    .reset_index()
)

# Create a new column with formatted quantile labels that include the salary range
salary_ranges["quantile_label"] = salary_ranges.apply(
    lambda row: f"{row['Salary_Quantile']} (${row['min']:.0f} - ${row['max']:.0f})",
    axis=1,
)

# Merge the salary range labels back into the original dataframe
df = df.merge(
    salary_ranges[["Salary_Quantile", "quantile_label"]],
    on="Salary_Quantile",
    how="left",
)

# Calculate min, max, mean, and std CF_Percent for each salary quantile within each season
quantile_stats = (
    df.groupby(["season", "Salary_Quantile"], observed=False)["CF_Percent"]
    .agg(["min", "max", "mean", "std"])
    .reset_index()
)

# Scatter plot with faceting by season
scatter_plot = (
    alt.Chart(df)
    .mark_circle(size=60)
    .encode(
        x=alt.X(
            "CF_Percent",
            title="Corsi For Percentage (CF%)",
            scale=alt.Scale(domain=[34, 67]),
        ),
        y=alt.Y("capHit", title="Cap Hit ($)", scale=alt.Scale(zero=False)),
        color=alt.Color("quantile_label:N", title="Salary Quantile (Range)"),
        tooltip=["player_id", "CF_Percent", "capHit", "quantile_label"],
    )
    .properties(
        width=288,  # Set width for each panel
        height=336,
    )
    .facet(
        column=alt.Column(
            "season:N", title="Season", header=alt.Header(labelAngle=0)
        )  # Facet by season with horizontal layout
    )
)

# Create a table displaying the CF_Percent statistics (min, max, mean, std) for each season
cf_percent_table = (
    alt.Chart(quantile_stats)
    .transform_fold(
        ["min", "max", "mean", "std"],  # Fold the columns into rows
        as_=["Statistic", "Value"],
    )
    .mark_text(align="left", baseline="middle", dx=5, fontSize=14)
    .encode(
        x=alt.X(
            "Statistic:N",
            title="CF_Percent Stats",
            sort=["min", "max", "mean", "std"],
            axis=alt.Axis(labelAngle=-45),
        ),
        y=alt.Y(
            "Salary_Quantile:N", title="Salary Quantile"
        ),  # Quantiles as y-axis categories
        text=alt.Text("Value:Q", format=".2f"),  # Format the values
    )
    .properties(
        width=288,  # Set width to match the scatter plot panels
        height=100,
    )
    .facet(
        column=alt.Column(
            "season:N", title=None, header=alt.Header(labelAngle=0)
        )  # Align the table with the scatter plot by season
    )
)

# Vertically concatenate the scatter plot and statistical table
combined_chart = alt.vconcat(
    scatter_plot,  # The scatter plot for each season
    cf_percent_table,  # The CF_Percent table for each season
)

# Enable default rendering
alt.renderers.enable("default")

# Display the combined chart
combined_chart
