"""Code to run necessary sequence for updating db tables and views."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SEASONS_MODERN = [20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025]

REPO = Path(__file__).resolve().parent
CANON_SQL = REPO / "sql" / "canonicalize_ids.sql"


def run(cmd: list[str]) -> None:
    print("\n>>>", " ".join(cmd))
    subprocess.run(cmd, check=True)


def run_psql(psql_dsn: str, sql: str) -> None:
    # Runs a psql snippet with ON_ERROR_STOP enabled
    run(["psql", psql_dsn, "-v", "ON_ERROR_STOP=1", "-c", sql])


def run_psql_file(psql_dsn: str, season: int, sql_path: Path) -> None:
    # Set season var and run file
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--dsn", required=True, help="psql DSN string, e.g. postgresql://...")
    args = ap.parse_args()

    seasons = [args.season] if args.season else SEASONS_MODERN

    for s in seasons:
        print(f"\n==================== {s} ====================")

        # 1) Build ES
        run([sys.executable, "build_player_game_es.py", "--season", str(s)])

        # 2) Canonicalize ES IDs
        run_psql_file(args.dsn, s, CANON_SQL)

        # 3) Rebuild stats/toi_total
        run([sys.executable, "rebuild_player_game_stats_all_modern.py", "--season", str(s)])

        # 4) Build truth features
        run_psql(args.dsn, f"CALL mart.build_player_game_features_truth({s});")

        # 5) Rebuild raw corsi
        run(
            [
                sys.executable,
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
