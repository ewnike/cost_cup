"""
July 30, 2024
Code to load data from database into working env
so that code can access data, calculate corsi,
aggregate data, and insert data back into data table.
Eric Winiecke.
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_env_vars():
    """Assemble credentials for login."""
    load_dotenv()
    env_vars = {
        "DATABASE_TYPE": os.getenv("DATABASE_TYPE"),
        "DBAPI": os.getenv("DBAPI"),
        "ENDPOINT": os.getenv("ENDPOINT"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "PORT": int(os.getenv("PORT", 5432)),
        "DATABASE": os.getenv("DATABASE"),
    }
    return env_vars


def get_db_engine(env_vars):
    """Create connection string to database."""
    connection_string = (
        f"{env_vars['DATABASE_TYPE']}+{env_vars['DBAPI']}://"
        f"{env_vars['USER']}:{env_vars['PASSWORD']}@"
        f"{env_vars['ENDPOINT']}:{env_vars['PORT']}/"
        f"{env_vars['DATABASE']}"
    )
    return create_engine(connection_string)


# load_data.py

# load_data.py


def fetch_game_ids_20152016(engine):
    """Fetch all game IDs for the 2015-2016 season from the database."""
    query = """
    SELECT DISTINCT game_id
    FROM public.game_plays
    WHERE game_id >= 2015000000
    AND game_id < 2016000000;
    """
    return pd.read_sql(query, engine)["game_id"].tolist()


def load_data(env_vars):
    """Connect to db."""
    engine = get_db_engine(env_vars)

    queries = {
        "game_skater_stats": "SELECT * FROM game_skater_stats",
        "game_plays": "SELECT * FROM game_plays",
        "game_shifts": "SELECT * FROM game_shifts",
        "game": "SELECT * FROM game",
    }

    df = {}
    for name, query in queries.items():
        df[name] = pd.read_sql(query, engine)

        # Ensure 'game_id' is cast to int64 for consistency
        if "game_id" in df[name].columns:
            df[name]["game_id"] = df[name]["game_id"].astype("int64")

        print(f"{name}:")
        print(df[name].head())  # Print first few rows of each DataFrame for debugging

    return df


if __name__ == "__main__":
    env_vars = get_env_vars()
    df = load_data(env_vars)

    print("Data loaded successfully.")
    for name, df in df.items():
        print(f"{name}: {len(df)} rows")
