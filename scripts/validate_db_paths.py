"""
Validate required raw.* tables and columns exist and perform a small smoke test.

Checks that required tables/columns exist in the database and runs a smoke test query for
TEST_GAME_ID to ensure the raw ingestion tables are populated and readable.
"""

from __future__ import annotations

import sys

import pandas as pd

from db_utils import get_db_engine
from schema_utils import fq

REQUIRED = {
    ("raw", "game"): ["game_id", "season"],
    ("raw", "game_plays"): [
        "game_id",
        "event",
        "period",
        "periodTime",
        "team_id_for",
        "team_id_against",
    ],
    ("raw", "game_shifts"): ["game_id", "player_id", "period", "shift_start", "shift_end"],
    ("raw", "game_skater_stats"): ["game_id", "player_id", "team_id"],
}

TEST_GAME_ID = 2015020001


def table_exists(engine, schema: str, table: str) -> bool:  # noqa: D103
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = %(schema)s AND table_name = %(table)s
    LIMIT 1
    """
    return not pd.read_sql(q, engine, params={"schema": schema, "table": table}).empty


def columns(engine, schema: str, table: str) -> set[str]:  # noqa: D103
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %(schema)s AND table_name = %(table)s
    """
    df = pd.read_sql(q, engine, params={"schema": schema, "table": table})
    return set(df["column_name"].tolist())


def main() -> int:
    """Validate db paths."""
    engine = get_db_engine()
    try:
        print("=== Validating raw.* tables ===")
        for (schema, table), cols in REQUIRED.items():
            if not table_exists(engine, schema, table):
                print(f"❌ Missing table: {schema}.{table}")
                return 2
            have = columns(engine, schema, table)
            missing = [c for c in cols if c not in have]
            if missing:
                print(f"❌ Missing columns in {schema}.{table}: {missing}")
                return 3
            print(f"✅ {schema}.{table} OK")

        print("\n=== Smoke test: load one legacy game ===")
        game_tbl = fq("raw", "game")
        plays_tbl = fq("raw", "game_plays")
        shifts_tbl = fq("raw", "game_shifts")
        gss_tbl = fq("raw", "game_skater_stats")

        g = pd.read_sql(
            f"SELECT * FROM {game_tbl} WHERE game_id=%(gid)s", engine, params={"gid": TEST_GAME_ID}
        )
        p = pd.read_sql(
            f"SELECT * FROM {plays_tbl} WHERE game_id=%(gid)s", engine, params={"gid": TEST_GAME_ID}
        )
        s = pd.read_sql(
            f"SELECT * FROM {shifts_tbl} WHERE game_id=%(gid)s",
            engine,
            params={"gid": TEST_GAME_ID},
        )
        ss = pd.read_sql(
            f"SELECT * FROM {gss_tbl} WHERE game_id=%(gid)s", engine, params={"gid": TEST_GAME_ID}
        )

        print(f"raw.game rows: {len(g)}")
        print(f"raw.game_plays rows: {len(p)}")
        print(f"raw.game_shifts rows: {len(s)}")
        print(f"raw.game_skater_stats rows: {len(ss)}")

        if len(g) != 1 or len(p) == 0 or len(s) == 0 or len(ss) == 0:
            print("❌ Smoke test failed: unexpected row counts.")
            return 4

        print("✅ Smoke test passed.")
        return 0

    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
