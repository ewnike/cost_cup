import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)

# Load environment variables from the .env file
load_dotenv()

# Retrieve database connection parameters from environment variables
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5432))  # Provide default value if not set
DATABASE = os.getenv("DATABASE")

# Create the connection string
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)
engine = create_engine(connection_string)


# Define function to get data from the database
def get_data_from_db(query):
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


# Define function to create a new table schema for aggregated data
def create_aggregated_table(table_name):
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column("player_id", BigInteger, primary_key=True),
        Column("firstName", String),
        Column("lastName", String),
        Column("corsi_for", Float),
        Column("corsi_against", Float),
        Column("corsi", Float),
        Column("CF_Percent", Float),
        Column("timeOnIce", Float),
        Column("game_count", Integer),
        Column("Cap_Hit", String(50)),
    )
    metadata.create_all(engine)


for season in ["20152016", "20162017", "20172018"]:
    # Get corsi data
    corsi_query = f"SELECT * FROM raw_corsi_{season}"
    df_corsi = get_data_from_db(corsi_query).drop(columns=["Unnamed: 0"])

    # Get game skater stats
    gss_toi_query = "SELECT game_id, player_id, timeOnIce FROM gss_toi"
    df_gss_toi = get_data_from_db(gss_toi_query)

    # Get player info
    player_info_query = (
        "SELECT player_id, firstName, lastName, primaryPosition FROM player_info"
    )
    df_player_info = get_data_from_db(player_info_query)

    # Merge dataframes
    df_all = pd.merge(df_corsi, df_gss_toi, on=["game_id", "player_id"])
    df_all = pd.merge(df_all, df_player_info, on="player_id")

    # Group and aggregate player stats
    df_grouped_all = (
        df_all.groupby("player_id")
        .agg(
            {
                "firstName": "first",
                "lastName": "first",
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

    # Get player salary data
    player_salary_query = (
        f"SELECT firstName, lastName, capHit FROM player_cap_hit_{season}"
    )
    df_player_salary = get_data_from_db(player_salary_query)

    # Merge aggregated stats with salary info
    df_grouped_all = pd.merge(
        df_grouped_all, df_player_salary, on=["firstName", "lastName"]
    )

    # Post-processing
    df_grouped_all["CF_Percent"] = df_grouped_all["CF_Percent"].round(4) * 100
    threshold = 82 * 0.32
    df_grouped_all = df_grouped_all.query(f"game_count >= {threshold}")
    df_grouped_all = df_grouped_all.sort_values("CF_Percent", ascending=False)

    # Define table name for aggregated data
    aggregated_table_name = f"aggregated_corsi_{season}"

    # Create new table for aggregated data
    create_aggregated_table(aggregated_table_name)

    # Insert aggregated data into the new table
    df_grouped_all.to_sql(
        aggregated_table_name, con=engine, if_exists="replace", index=False
    )

    print(f"Data inserted successfully into {aggregated_table_name}")

print("Data inserted successfully into all tables")
