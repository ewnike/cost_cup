#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine, text
import pandas as pd

def make_engine(user, password, host, port, db):
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        connect_args={"sslmode": "require"},
        pool_pre_ping=True,
    )

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                current_database() AS db,
                current_user       AS user,
                inet_server_addr() AS host,
                inet_server_port() AS port
        """)).mappings().fetchone()

        print(
            f"Connected to {row['db']} "
            f"as {row['user']} "
            f"@ {row['host']}:{row['port']}"
        )

    return engine
