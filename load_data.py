"""
July 30, 2024
Code to load data from databas into workin env
so that code can access data, calculate corsi,
aggregate data, and insert data back into data table.
Eric Winiecke
"""

import os
from time import perf_counter

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def get_db_engine(env_vars):
    """
    get info to create connection string to database.
    """
    connection_string = (
        f"{env_vars['DATABASE_TYPE']}+{env_vars['DBAPI']}://"
        f"{env_vars['USER']}:{env_vars['PASSWORD']}@"
        f"{env_vars['ENDPOINT']}:{env_vars['PORT']}/"
        f"{env_vars['DATABASE']}"
    )
    return create_engine(connection_string)


def load_data_from_db(table_name, env_vars):
    """
    read in data
    """
    engine = get_db_engine(env_vars)
    query = text(f"SELECT * FROM {table_name}")
    return pd.read_sql(query, engine)


def load_data(env_vars):
    """
    loading data into a dictionary
    """
    names = ["game_skater_stats", "game_plays", "game_shifts", "game"]
    t2 = perf_counter()
    df = {}

    print("Loading data from PostgreSQL database...")
    for name in names:
        df[name] = load_data_from_db(name, env_vars).drop_duplicates(ignore_index=True)
        t1, t2 = t2, perf_counter()
        print(f"{name:>25}: {t2 - t1:.4g} sec, {len(df[name])} rows")

    return df


def get_env_vars():
    """
    log into database
    """
    load_dotenv()
    env_vars = {
        "DATABASE_TYPE": os.getenv("DATABASE_TYPE"),
        "DBAPI": os.getenv("DBAPI"),
        "ENDPOINT": os.getenv("ENDPOINT"),
        "USER": os.getenv("USER"),
        "PASSWORD": os.getenv("PASSWORD"),
        "PORT": os.getenv("PORT"),
        "DATABASE": os.getenv("DATABASE"),
    }
    return env_vars


if __name__ == "__main__":
    env_vars = get_env_vars()
    df = load_data(env_vars)
    print("Data loaded successfully.")
