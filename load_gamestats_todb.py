"""
July 7, 2024
code to load NHL game data
into postgres db MADS_NHL
"""

import pandas as pd
import os

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    MetaData,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv


# Load environment variables from the .env file
load_dotenv()

# Retrieve database connection parameters from environment variables
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5433))  # Provide default value if not set
DATABASE = os.getenv("DATABASE")

# Create the connection string
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)

engine = create_engine(connection_string)

directory = [r"C:\Users\eric\Documents\cost_of_cup\Kaggle_Big_stats"]

metadata = MetaData()

game = Table(
    "game",
    metadata,
    Column("game_id", BigInteger, primary_key=True),
    Column("season", Integer),
    Column("type", String(25)),
    Column("date_time_GMT", DateTime(timezone=True)),
    Column("away_team_id", Integer),
    Column("home_team_id", Integer),
    Column("away_goals", Integer),
    Column("home_goals", Integer),
    Column("outcome", String(50)),
    Column("home_rink_side_start", String(50)),
    Column("venue", String(50)),
    Column("venue_link", String(50)),
    Column("venue_time_zone_id", Integer),
    Column("venue_time_zone_offset", String(25)),
    Column("venue_time_zone_tz", String(25)),
)

game_plays = Table(
    "game_plays",
    metadata,
    Column("play_id", String(20)),
    Column("game_id", BigInteger),
    Column("team_id_for", Float, nullable=True),
    Column("team_id_against", Float, nullable=True),
    Column("event", String(50)),
    Column("secondaryType", String(50)),
    Column("x", Float, nullable=True),
    Column("y", Float, nullable=True),
    Column("period", Integer),
    Column("periodType", String(50)),
    Column("periodTime", BigInteger),
    Column("periodTimeRemaining", BigInteger),
    Column("dateTime", DateTime(timezone=False)),
    Column("goals_away", Integer, nullable=True),
    Column("goals_home", Integer, nullable=True),
    Column("description", String(50)),
    Column("st_x", Integer, nullable=True),
    Column("st_y", Integer, nullable=True),
)

game_shifts = Table(
    "game_shifts",
    metadata,
    Column("game_id", BigInteger),
    Column("player_id", BigInteger),
    Column("period", Integer),
    Column("shift_start", BigInteger),
    Column("shift_end", BigInteger),
)

game_skater_stats = Table(
    "game_skater_stats",
    metadata,
    Column("game_id", BigInteger),
    Column("player_id", BigInteger),
    Column("team_id", Integer),
    Column("timeOnIce", BigInteger),
    Column("assists", Integer),
    Column("goals", Integer),
    Column("shots", Integer),
    Column("hits", Integer),
    Column("powerPlayGoals", Integer),
    Column("powerPlayAssists", Integer),
    Column("penaltyMinutes", Integer),
    Column("faceOffWins", Integer),
    Column("faceoffTaken", Integer),
    Column("takeaways", Integer),
    Column("giveaways", Integer),
    Column("shortHandedGoals", Integer),
    Column("shortHandedAssists", Integer),
    Column("blocked", Integer),
    Column("plusMinus", Integer),
    Column("evenTimeOnIce", BigInteger),
    Column("shortHandedTimeOnIce", BigInteger),
    Column("powerPlayTimeOnIce", BigInteger),
)

# session.add(new_game_play)
tables = [game, game_plays, game_shifts, game_skater_stats]
metadata.create_all(engine)

Session = sessionmaker(bind=engine)


def insert_data_from_csv(session, table, directory, column_mapping):
    try:
        df = pd.read_csv(directory)
        for index, row in df.iterrows():
            data = {
                column: row[csv_column] for column, csv_column in column_mapping.items()
            }
            session.execute(table.insert().values(**data))
        session.commit()
        print(f"Data inserted successfully into {table.name}")

    except SQLALchemyError:
        session.rollback()
        print(f"error inserting data into {table.name}")
    except FileNotFoundError as e:
        print(f"File not found: {file_path} - {e}")


csv_files_and_mappings = [
    (
        r"C:\Users\eric\Documents\cost_of_cup\Kaggle_Big_stats\game.csv",
        game,
        {
            "game_id": "game_id",
            "season": "season",
            "type": "type",
            "date_time_GMT": "date_time_GMT",
            "away_team_id": "away_team_id",
            "home_team_id": "home_team_id",
            "away_goals": "away_goals",
            "home_goals": "home_goals",
            "outcome": "outcome",
            "home_rink_side_start": "home_rink_side_start",
            "venue": "venue",
            "venue_link": "venue_link",
            "venue_time_zone_id": "venue_time_zone_id",
            "venue_time_zone_offset": "venue_time_zone_offset",
            "venue_time_zone_tz": "venue_time_zone_tz",
        },
    ),
    (
        r"C:\Users\eric\Documents\cost_of_cup\Kaggle_Big_stats\game_plays.csv",
        game_plays,
        {
            "play_id": "play_id",
            "game_id": "game_id",
            "team_id_for": "team_id_for",
            "team_id_against": "team_id_against",
            "event": "event",
            "secondaryType": "secondaryType",
            "x": "x",
            "y": "y",
            "period": "period",
            "periodType": "periodType",
            "periodTime": "periodTime",
            "periodTimeRemaining": "periodTimeRemaining",
            "dateTime": "dateTime",
            "goals_away": "goals_away",
            "goals_home": "goals_home",
        },
    ),
    (
        r"C:\Users\eric\Documents\cost_of_cup\Kaggle_Big_stats\game_shifts.csv",
        game_shifts,
        {
            "game_id": "game_id",
            "player_id": "player_id",
            "period": "period",
            "shift_start": "shift_start",
            "shift_end": "shift_end",
        },
    ),
    (
        r"C:\Users\eric\Documents\cost_of_cup\Kaggle_Big_stats\game_skater_stats.csv",
        game_skater_stats,
        {
            "player_id": "player_id",
            "game_id": "game_id",
            "team_id": "team_id",
            "timeOnIce": "timeOnIce",
            "assists": "assists",
            "goals": "goals",
            "shots": "shots",
            "hits": "hits",
            "powerPlayGoals": "powerPlayGoals",
            "powerPlayAssists": "powerPlayAssists",
            "penaltyMinutes": "penaltyMinutes",
            "faceOffWins": "faceOffWins",
            "faceoffTaken": "faceoffTaken",
            "takeaways": "takeaways",
            "giveaways": "giveaways",
            "shortHandedGoals": "shortHandedGoals",
            "shortHandedAssists": "shortHandedAssists",
            "blocked": "blocked",
            "plusMinus": "plusMinus",
            "evenTimeOnIce": "evenTimeOnIce",
            "shortHandedTimeOnIce": "shortHandedTimeOnIce",
            "powerPlayTimeOnIce": "powerPlayTimeOnIce",
        },
    ),
]
with Session() as session:
    for directory, table, column_mapping in csv_files_and_mappings:
        insert_data_from_csv(session, table, directory, column_mapping)

    print("data inserted successfully into all tables")
