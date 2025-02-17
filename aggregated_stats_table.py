"""
August 11, 2024.

code to create aggregated table in database.
The code also aggregates the stats per season
per player.

Eric Winiecke.
"""

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
)

from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()


# Define function to get data from the database
def get_data_from_db(query):
    """
    Execute an SQL query and return the results as a Pandas DataFrame.

    Args:
    ----
        query (str): The SQL query to execute.

    Returns:
    -------
        pd.DataFrame: A DataFrame containing the query results.

    Example:
    -------
        ```
        python
        df = get_data_from_db("SELECT * FROM player_stats")
        print(df.head())
        ```

    """
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


def create_aggregated_table(table_name):
    """
    Create an aggregated table in the database for storing hockey player statistics.

    Args:
    ----
        table_name (str): The name of the table to be created.

    Example:
    -------
        ```
        python
        create_aggregated_table("aggregated_player_stats")
        ```

    """
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column("player_id", BigInteger, primary_key=True),
        Column("firstName", String),
        Column("lastName", String),
        Column("team_id", String),  # Added team_id
        Column("corsi_for", Float),
        Column("corsi_against", Float),
        Column("corsi", Float),
        Column("CF_Percent", Float),
        Column("timeOnIce", Float),
        Column("game_count", Integer),
        Column("Cap_Hit", Float),
    )
    metadata.create_all(engine)


for season in ["20152016", "20162017", "20172018"]:
    # Get corsi data
    CORSI_QUERY = f"SELECT * FROM raw_corsi_{season}"
    df_corsi = get_data_from_db(CORSI_QUERY)
    if "Unnamed: 0" in df_corsi.columns:
        df_corsi = df_corsi.drop(columns=["Unnamed: 0"])

    # Get game skater stats
    GSS_TOI_QUERY = 'SELECT game_id, player_id, "timeOnIce", team_id FROM game_skater_stats'
    df_gss_toi = get_data_from_db(GSS_TOI_QUERY)

    # Get player info
    PLAYER_INFO_QUERY = (
        'SELECT player_id, "firstName", "lastName", "primaryPosition" FROM player_info'
    )
    df_player_info = get_data_from_db(PLAYER_INFO_QUERY)

    # Drop 'team_id' from df_corsi to avoid duplication
    df_corsi.drop(columns=["team_id"], inplace=True, errors="ignore")

    # Merge dataframes
    df_all = pd.merge(df_corsi, df_gss_toi, on=["game_id", "player_id"])
    df_all = pd.merge(df_all, df_player_info, on="player_id")

    # Verify the columns after merging
    print(f"Columns in df_all before grouping for season {season}: {df_all.columns}")

    # Group and aggregate player stats
    df_grouped_all = (
        df_all.groupby("player_id")
        .agg(
            {
                "firstName": "first",
                "lastName": "first",
                "team_id": "first",  # Include team_id
                "corsi_for": "mean",
                "corsi_against": "mean",
                "corsi": "mean",
                "CF_Percent": "mean",
                "timeOnIce": "mean",
                "game_id": "count",
            }
        )
        .reset_index()
        .rename(columns={"game_id": "game_count"})
    )

    PLAYER_SALARY_QUERY = f'SELECT "firstName", "lastName", "capHit" FROM player_cap_hit_{season}'

    df_player_salary = get_data_from_db(PLAYER_SALARY_QUERY)

    # Convert capHit from string to float
    df_player_salary["capHit"] = (
        df_player_salary["capHit"].replace(r"[\$,]", "", regex=True).astype(float)
    )

    # Merge aggregated stats with salary info
    df_grouped_all = pd.merge(df_grouped_all, df_player_salary, on=["firstName", "lastName"])

    # Round all relevant columns to four decimal places
    df_grouped_all["corsi_for"] = df_grouped_all["corsi_for"].round(4)
    df_grouped_all["corsi_against"] = df_grouped_all["corsi_against"].round(4)
    df_grouped_all["corsi"] = df_grouped_all["corsi"].round(4)
    df_grouped_all["CF_Percent"] = (
        (
            (
                df_grouped_all["corsi_for"]
                / (df_grouped_all["corsi_for"] + df_grouped_all["corsi_against"])
            )
            * 100
        )
        .fillna(0)
        .round(4)
    )

    df_grouped_all["timeOnIce"] = df_grouped_all["timeOnIce"].round(4)

    # Apply the threshold for game_count
    THRESHOLD = 82 * 0.32
    df_grouped_all = df_grouped_all.query(f"game_count >= {THRESHOLD}")

    # Sort by CF_Percent in descending order
    df_grouped_all = df_grouped_all.sort_values("CF_Percent", ascending=False)

    # Define table name for aggregated data
    AGGREGATED_TABLE_NAME = f"aggregated_corsi_{season}"

    # Create new table for aggregated data
    create_aggregated_table(AGGREGATED_TABLE_NAME)

    # Insert aggregated data into the new table
    df_grouped_all.to_sql(AGGREGATED_TABLE_NAME, con=engine, if_exists="replace", index=False)

    print(f"Data inserted successfully into {AGGREGATED_TABLE_NAME}")

print("Data inserted successfully into all tables")
