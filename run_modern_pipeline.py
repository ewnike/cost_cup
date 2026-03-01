"""
Run the modern pipeline end-to-end (Python + psql), streaming output live.

Per season:
  1) Build ES: mart.player_game_es_{season}
  2) Canonicalize IDs (psql): sql/mart/canonicalize_ids.sql
     - FAIL FAST if canonicalize creates duplicate (game_id,player_id,team_id) keys in ES
  3) Rebuild stats/toi_total: mart.player_game_stats_{season}, mart.toi_total_{season}
  4) Build truth features (psql): CALL mart.build_player_game_features_truth(season)
  5) Rebuild raw corsi: derived.raw_corsi_{season}

Notes:
- Uses `-u` for Python scripts so prints/logs flush immediately.
- Uses psql ON_ERROR_STOP so SQL failures stop the pipeline.

"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SEASONS_MODERN = [20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025]

REPO = Path(__file__).resolve().parent
CANON_SQL = REPO / "sql" / "mart" / "canonicalize_ids.sql"
BOX_ES_SQL = REPO / "sql" / "mart" / "build_player_game_boxscore_es.sql"


def run(cmd: list[str]) -> None:
    """Run a command, streaming stdout/stderr live; fail fast on non-zero exit."""
    cmd_str = " ".join(map(str, cmd))
    print(f"\n>>> {cmd_str}")
    subprocess.run(cmd, check=True)


def run_psql(psql_dsn: str, sql: str) -> None:
    """Run a one-off SQL snippet via psql with ON_ERROR_STOP enabled."""
    run(["psql", psql_dsn, "-v", "ON_ERROR_STOP=1", "-c", sql])


def run_psql_file(psql_dsn: str, season: int, sql_path: Path) -> None:
    """Run a SQL file via psql, setting :season and stopping on first error."""
    run(
        [
            "psql",
            psql_dsn,
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            f"season={season}",
            "-f",
            str(sql_path),
        ]
    )


def fail_if_es_dupes(psql_dsn: str, season: int) -> None:
    """Raise immediately if ES has duplicate (game_id,player_id,team_id) keys."""
    run_psql(
        psql_dsn,
        f"""
        DO $$
        DECLARE n_dupe bigint;
        BEGIN
          SELECT COUNT(*) INTO n_dupe
          FROM (
            SELECT game_id, player_id, team_id
            FROM mart.player_game_es_{season}
            GROUP BY 1,2,3
            HAVING COUNT(*) > 1
          ) d;

          IF n_dupe > 0 THEN
            RAISE EXCEPTION 'season %: mart.player_game_es_% has % duplicate keys',
              {season}, {season}, n_dupe;
          END IF;
        END $$;
        """,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--season", type=int, default=None, help="Run one season, e.g. 20242025"
    )
    ap.add_argument(
        "--dsn", required=True, help="psql DSN string, e.g. postgresql://..."
    )
    args = ap.parse_args()

    seasons = [args.season] if args.season is not None else SEASONS_MODERN

    for s in seasons:
        print(f"\n==================== {s} ====================")

        # 1) Build ES
        run([sys.executable, "-u", "build_player_game_es.py", "--season", str(s)])
        fail_if_es_dupes(args.dsn, s)  # safety check

        # 2) Canonicalize ES IDs
        run_psql_file(args.dsn, s, CANON_SQL)
        fail_if_es_dupes(args.dsn, s)  # FAIL FAST if canonicalize introduces dupes

        # 3) Rebuild stats/toi_total
        run(
            [
                sys.executable,
                "-u",
                "rebuild_player_game_stats_all_modern.py",
                "--season",
                str(s),
            ]
        )

        # 3.5) Build 5v5 ES boxscore keyed to ES
        run_psql_file(args.dsn, s, BOX_ES_SQL)

        # 4) Build truth features
        run_psql(args.dsn, f"CALL mart.build_player_game_features_truth({s});")

        # 5) Rebuild raw corsi
        run(
            [
                sys.executable,
                "-u",
                "rebuild_raw_corsi_all_modern.py",
                "--season",
                str(s),
                "--schema",
                "derived",
            ]
        )

    print("\n✅ Done")


if __name__ == "__main__":
    main()
