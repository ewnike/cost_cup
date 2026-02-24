"""
Run the modern archetypes pipeline (psql + python), streaming output live.

Steps:
  0) OPTIONAL: refresh mart.player_game_features_{season} via stored proc
  1) Build/refresh mart.player_season_features_modern_truth (per-season append table)
     - sql/mart/player_season_features_modern_truth.sql (expects :season)
  2) Build mart.player_season_archetype_features_modern_truth (all seasons)
     - sql/mart/player_season_archetype_features_modern_truth.sql
  3) Build mart.player_season_archetype_features_modern_truth_clean (all seasons)
     - sql/mart/player_season_archetype_features_modern_truth_clean.sql
  4) Cluster
     - python cluster_player_archetypes_modern.py

Usage:
  python run_archetypes_pipeline.py --dsn "host=... port=... dbname=... user=... sslmode=require"
  python run_archetypes_pipeline.py --dsn "..." --season 20242025
  python run_archetypes_pipeline.py --dsn "..." --skip-game-features
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SEASONS_MODERN = [20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025]

REPO = Path(__file__).resolve().parent
SQL_DIR = REPO / "sql" / "mart"

PLAYER_SEASON_SQL = SQL_DIR / "player_season_features_modern_truth.sql"
ARCH_SQL = SQL_DIR / "player_season_archetype_features_modern_truth.sql"
CLEAN_SQL = (
    SQL_DIR / "player_season_archetype_features_modern_truth_clean.sql"
)  # make sure this exists


def run(cmd: list[str]) -> None:
    """Run a command, streaming stdout/stderr live; fail fast on non-zero exit."""
    cmd_str = " ".join(map(str, cmd))
    print(f"\n>>> {cmd_str}")
    subprocess.run(cmd, check=True)


def run_psql(psql_dsn: str, sql: str) -> None:
    """Run a one-off SQL snippet via psql with ON_ERROR_STOP enabled."""
    run(["psql", psql_dsn, "-v", "ON_ERROR_STOP=1", "-c", sql])


def run_psql_file(psql_dsn: str, season: int | None, sql_path: Path) -> None:
    """Run a SQL file via psql, optionally setting :season and stopping on first error."""
    cmd = ["psql", psql_dsn, "-v", "ON_ERROR_STOP=1"]
    if season is not None:
        cmd += ["-v", f"season={season}"]
    cmd += ["-f", str(sql_path)]
    run(cmd)


def fail_if_clean_missing(psql_dsn: str) -> None:
    run_psql(
        psql_dsn,
        """
        DO $$
        BEGIN
          IF to_regclass('mart.player_season_archetype_features_modern_truth_clean') IS NULL THEN
            RAISE EXCEPTION 'Missing mart.player_season_archetype_features_modern_truth_clean';
          END IF;
        END $$;
        """,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None, help="Run one season, e.g. 20242025")
    ap.add_argument(
        "--dsn", required=True, help="psql DSN string, e.g. host=... dbname=... user=..."
    )
    ap.add_argument(
        "--skip-game-features",
        action="store_true",
        help="Skip calling mart.build_player_game_features_truth(season)",
    )
    args = ap.parse_args()

    seasons = [args.season] if args.season is not None else SEASONS_MODERN

    # 0) OPTIONAL: refresh player_game_features_{season} views/tables via proc
    if not args.skip_game_features:
        for s in seasons:
            print(f"\n==================== refresh game features {s} ====================")
            run_psql(args.dsn, f"CALL mart.build_player_game_features_truth({s});")

    # 1) player_season_features_modern_truth (per-season write into mart append table)
    for s in seasons:
        print(f"\n==================== player_season_features {s} ====================")
        run_psql_file(args.dsn, s, PLAYER_SEASON_SQL)

    # 2) archetype features (all seasons table)
    print("\n==================== archetype features truth ====================")
    run_psql_file(args.dsn, None, ARCH_SQL)

    # 3) clean table
    print("\n==================== archetype features clean ====================")
    run_psql_file(args.dsn, None, CLEAN_SQL)
    fail_if_clean_missing(args.dsn)

    # 4) cluster
    print("\n==================== clustering ====================")
    run([sys.executable, "-u", "cluster_player_archetypes_modern.py"])

    print("\n✅ Archetypes pipeline done")


if __name__ == "__main__":
    main()
