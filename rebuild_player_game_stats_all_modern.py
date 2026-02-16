"""
Rebuild modern mart.player_game_stats_{season} for all SEASONS_MODERN.

Builds:
  1) mart.toi_total_{season} from derived.raw_shifts_resolved (skaters only, all strengths)
  2) mart.player_game_stats_{season} from mart.player_game_es_{season} (authoritative keyset)
     LEFT JOIN toi_total_{season} for toi_total_sec

Guarantees:
  stats_rows == es_rows for every season (or raises an error)

Usage:
  python rebuild_player_game_stats_all_modern.py
  python rebuild_player_game_stats_all_modern.py --season 20192020
  python rebuild_player_game_stats_all_modern.py --drop-toi-total
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
        )
        OR EXISTS (
          SELECT 1
          FROM information_schema.views
          WHERE table_schema = :schema
            AND table_name = :table
        );
        """
    )
    return bool(conn.execute(q, {"schema": schema, "table": table}).scalar())


def rebuild_for_season(*, season: int, drop_toi_total: bool) -> None:
    engine = get_db_engine()

    es_schema = "mart"
    es_table = f"player_game_es_{season}"

    toi_schema = "mart"
    toi_table = f"toi_total_{season}"

    out_schema = "mart"
    out_table = f"player_game_stats_{season}"

    shifts_view = '"derived"."raw_shifts_resolved"'
    dim_team = '"dim"."dim_team_code"'

    try:
        with engine.begin() as conn:
            if not table_exists(conn, es_schema, es_table):
                logger.warning("⚠️ %s: missing %s.%s; skipping", season, es_schema, es_table)
                return

            # 1) Total TOI per (game_id, player_id, team_id) from shifts (skaters only)
            logger.info("%s: building %s.%s", season, toi_schema, toi_table)

            conn.execute(text(f'DROP TABLE IF EXISTS "{toi_schema}"."{toi_table}";'))

            conn.execute(
                text(
                    f"""
                    CREATE TABLE "{toi_schema}"."{toi_table}" AS
                    SELECT
                      {season}::int AS season,
                      rs.game_id::bigint AS game_id,
                      rs.player_id_resolved::bigint AS player_id,
                      dt.team_id::bigint AS team_id,
                      SUM(GREATEST(0, rs.seconds_end - rs.seconds_start))::bigint AS toi_total_sec
                    FROM {shifts_view} rs
                    JOIN {dim_team} dt
                      ON dt.team_code = rs.team
                    WHERE rs.season = {season}
                      AND rs.session = 'R'
                      AND rs.position <> 'G'
                      AND rs.seconds_end > rs.seconds_start
                    GROUP BY 1,2,3,4;
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS toi_total_{season}_key_idx
                    ON "{toi_schema}"."{toi_table}" (game_id, player_id, team_id);
                    """
                )
            )

            # 2) Player-game stats from ES keyset + total TOI
            logger.info("%s: building %s.%s", season, out_schema, out_table)

            conn.execute(text(f'DROP TABLE IF EXISTS "{out_schema}"."{out_table}";'))

            conn.execute(
                text(
                    f"""
                    CREATE TABLE "{out_schema}"."{out_table}" AS
                    SELECT
                      {season}::int AS season,
                      es.game_id::bigint AS game_id,
                      es.player_id::bigint AS player_id,
                      es.team_id::bigint AS team_id,
                      es.cf::bigint AS cf,
                      es.ca::bigint AS ca,
                      COALESCE(tt.toi_total_sec, 0)::bigint AS toi_total_sec,
                      es.toi_sec::bigint AS toi_es_sec,
                      es.cf60::double precision AS cf60,
                      es.ca60::double precision AS ca60,
                      es.cf_percent::double precision AS cf_percent
                    FROM "{es_schema}"."{es_table}" es
                    LEFT JOIN "{toi_schema}"."{toi_table}" tt
                      ON tt.game_id = es.game_id
                     AND tt.player_id = es.player_id
                     AND tt.team_id = es.team_id;
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS player_game_stats_{season}_game_idx
                    ON "{out_schema}"."{out_table}" (game_id);
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS player_game_stats_{season}_player_idx
                    ON "{out_schema}"."{out_table}" (player_id);
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS player_game_stats_{season}_team_idx
                    ON "{out_schema}"."{out_table}" (team_id);
                    """
                )
            )

            # 3) Validate counts match ES exactly
            es_rows = conn.execute(
                text(f'SELECT COUNT(*) FROM "{es_schema}"."{es_table}";')
            ).scalar_one()
            stats_rows = conn.execute(
                text(f'SELECT COUNT(*) FROM "{out_schema}"."{out_table}";')
            ).scalar_one()

            if es_rows != stats_rows:
                raise RuntimeError(
                    f"{season}: row mismatch (es_rows={es_rows}, stats_rows={stats_rows})"
                )

            logger.info("✅ %s: stats_rows=%s (matches ES)", season, stats_rows)

            # 4) Optional cleanup
            if drop_toi_total:
                conn.execute(text(f'DROP TABLE IF EXISTS "{toi_schema}"."{toi_table}";'))
                logger.info("%s: dropped %s.%s", season, toi_schema, toi_table)

    finally:
        engine.dispose()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None, help="Run one season, e.g. 20192020")
    ap.add_argument(
        "--drop-toi-total",
        action="store_true",
        help="Drop mart.toi_total_{season} after building stats table",
    )
    args = ap.parse_args()

    seasons = [args.season] if args.season else [int(s) for s in SEASONS_MODERN]

    for s in seasons:
        rebuild_for_season(season=s, drop_toi_total=args.drop_toi_total)


if __name__ == "__main__":
    main()
