"""
Rebuild derived.raw_corsi_{season} for all modern seasons from mart.player_game_es_{season}.

raw_corsi_{season} is player-game grain and matches ES rowcount.

Usage:
  python rebuild_raw_corsi_all_modern.py
  python rebuild_raw_corsi_all_modern.py --season 20192020
"""

from __future__ import annotations

import argparse
import os
import pathlib

from sqlalchemy import text

from constants import SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

logger = setup_logger()


def table_exists(conn, schema: str, table: str) -> bool:
    q = text(
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = :schema
            AND table_name = :table
        );
        """
    )
    return bool(conn.execute(q, {"schema": schema, "table": table}).scalar())


def rebuild_for_season(*, season: int, out_schema: str = "derived") -> None:
    engine = get_db_engine()
    es_schema = "mart"
    es_table = f"player_game_es_{season}"
    out_table = f"raw_corsi_{season}"

    try:
        with engine.begin() as conn:
            if not table_exists(conn, es_schema, es_table):
                logger.warning("⚠️ %s: missing %s.%s; skipping", season, es_schema, es_table)
                return

            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{out_schema}";'))
            conn.execute(text(f'DROP TABLE IF EXISTS "{out_schema}"."{out_table}";'))

            conn.execute(
                text(
                    f"""
                    CREATE TABLE "{out_schema}"."{out_table}" AS
                    SELECT
                      es.game_id::bigint AS game_id,
                      es.player_id::bigint AS player_id,
                      es.team_id::bigint AS team_id,
                      es.cf::bigint AS corsi_for,
                      es.ca::bigint AS corsi_against,
                      (es.cf - es.ca)::bigint AS corsi,
                      es.cf_percent::double precision AS cf_percent
                    FROM "{es_schema}"."{es_table}" es;
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS raw_corsi_{season}_game_idx
                    ON "{out_schema}"."{out_table}" (game_id);
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS raw_corsi_{season}_player_idx
                    ON "{out_schema}"."{out_table}" (player_id);
                    """
                )
            )

            es_rows = conn.execute(
                text(f'SELECT COUNT(*) FROM "{es_schema}"."{es_table}";')
            ).scalar_one()
            out_rows = conn.execute(
                text(f'SELECT COUNT(*) FROM "{out_schema}"."{out_table}";')
            ).scalar_one()

            if es_rows != out_rows:
                raise RuntimeError(
                    f"{season}: row mismatch (es_rows={es_rows}, raw_corsi_rows={out_rows})"
                )

            logger.info(
                "✅ %s: rebuilt %s.%s rows=%s (matches ES)", season, out_schema, out_table, out_rows
            )

    finally:
        engine.dispose()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--schema", type=str, default="derived")
    args = ap.parse_args()

    seasons = [args.season] if args.season else [int(s) for s in SEASONS_MODERN]
    for s in seasons:
        rebuild_for_season(season=s, out_schema=args.schema)


if __name__ == "__main__":
    main()
