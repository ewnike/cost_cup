"""
Run the end-to-end Cost of Cup data pipeline.

Order:
  1) Validate config/paths
  2) Build derived tables (CTAS) that depend on raw tables
  3) Run job modules (if present)
"""

from __future__ import annotations

import os
import pathlib
import sys

from constants import SEASONS_MODERN
from db_utils import ctas_game_plays_from_raw_pbp, get_db_engine
from scripts.validate_db_paths import main as validate_main

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")


def _run_ctas_game_plays(engine) -> None:
    """Create derived.game_plays_{season}_from_raw_pbp for all modern seasons."""
    for season in SEASONS_MODERN:
        print(f"CTAS game_plays_from_raw_pbp for {season}")
        ctas_game_plays_from_raw_pbp(engine, int(season), drop=True)


# pylint: disable=import-outside-toplevel
def _run_jobs(engine) -> int:
    """
    Run pipeline jobs if modules exist.

    Return 0 if jobs ran (or are not wired yet); non-zero only for real failures.
    """
    try:
        from jobs.build_legacy_corsi import run_legacy_corsi
        from jobs.build_modern_player_game_es import run_modern_es
        from jobs.load_cap_hits import run_cap_hits
    except ModuleNotFoundError as exc:
        print("✅ Validation passed.")
        print("⚠️ Jobs not wired yet:", exc)
        return 0

    # Prefer passing engine if your jobs accept it (recommended)
    # If they don't yet, keep calling them without args.
    try:
        run_legacy_corsi(engine=engine)  # type: ignore[call-arg]
        run_modern_es(engine=engine)  # type: ignore[call-arg]
        run_cap_hits(engine=engine)  # type: ignore[call-arg]
    except TypeError:
        run_legacy_corsi()
        run_modern_es()
        run_cap_hits()

    return 0


def main() -> int:
    """Validate environment and run pipeline steps."""
    rc = validate_main()
    if rc != 0:
        return rc

    engine = get_db_engine()
    try:
        _run_ctas_game_plays(engine)
        return _run_jobs(engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
