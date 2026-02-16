"""
Rebuild derived.raw_corsi_{season} for modern seasons (20182019+)
from mart.player_game_stats_{season}.

Output schema/table (default):
  derived.raw_corsi_{season} with columns:
    season (int)
    player_id (bigint)
    corsi_for (bigint)
    corsi_against (bigint)
    cf_percent (double precision, 0-100)

Usage:
  python build_raw_corsi_modern.py                 # rebuild all SEASONS_MODERN
  python build_raw_corsi_modern.py --season 20182019
  python build_raw_corsi_modern.py --schema derived
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


def rebuild_one_season(*, season: int, schema_out: str = "derived") -> None:
    """Drop + recreate {schema_out}.raw_corsi_{season} from mart.player_game_stats_{season}."""
    engine = get_db_engine()
    src_table = f' "mart"."player_game_stats_{season}" '
    out_table = f' "{schema_out}"."raw_corsi_{season}" '

    sql_drop = f"DROP TABLE IF EXISTS {out_table};"

    # Keep schema stable (minimal columns) and cf_percent on 0-100 scale
    sql_create = f"""
    CREATE TABLE {out_table} AS
    SELECT
      {season}::int AS season,
      player_id::bigint AS player_id,
      SUM(cf)::bigint AS corsi_for,
      SUM(ca)::bigint AS corsi_against,
      CASE
        WHEN (SUM(cf) + SUM(ca)) > 0
          THEN 100.0 * SUM(cf)::double precision / (SUM(cf) + SUM(ca))
        ELSE 0.0
      END AS cf_percent
    FROM {src_table}
    GROUP BY player_id;
    """

    sql_index = f"""
    CREATE INDEX IF NOT EXISTS raw_corsi_{season}_player_id_idx
    ON {out_table} (player_id);
    """

    try:
        with engine.begin() as conn:
            # ensure schema exists
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_out}";'))

            conn.execute(text(sql_drop))
            conn.execute(text(sql_create))
            conn.execute(text(sql_index))

            n = conn.execute(text(f"SELECT COUNT(*) FROM {out_table};")).scalar_one()

        logger.info("✅ %s: rebuilt %s rows -> %s.raw_corsi_%s", season, n, schema_out, season)

    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--season", type=int, default=None, help="Rebuild one season, e.g. 20182019"
    )
    parser.add_argument(
        "--schema", type=str, default="derived", help="Output schema (default: derived)"
    )
    args = parser.parse_args()

    seasons = [args.season] if args.season else [int(s) for s in SEASONS_MODERN]

    for season in seasons:
        logger.info("Rebuilding raw_corsi for %s", season)
        rebuild_one_season(season=season, schema_out=args.schema)


if __name__ == "__main__":
    main()
