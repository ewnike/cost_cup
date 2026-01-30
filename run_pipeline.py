"""
Run the end-to-end Cost of Cup data pipeline.

This script:
  1) Validates local configuration and required paths/settings via `scripts.validate_db_paths`.
  2) If validation passes, runs the configured job steps (legacy Corsi build, modern ES build,
     and cap-hit load) when those job modules are available.

Notes:
  - Job modules are imported lazily so the pipeline can still run (and validate) even if
    the `jobs/` package is not yet fully wired.
  - Returns a process exit code (0 for success, non-zero for validation failure).

"""

import sys

from scripts.validate_db_paths import main as validate_main


def main() -> int:
    """
    Validate the environment and run pipeline jobs.

    Returns:
        Exit code: 0 on success; non-zero if validation fails.

    """
    rc = validate_main()
    if rc != 0:
        return rc

    # Lazy imports so pipeline runs even before jobs exist
    try:
        from jobs.build_legacy_corsi import (
            run_legacy_corsi,  # pylint: disable=import-outside-toplevel
        )
        from jobs.build_modern_player_game_es import (
            run_modern_es,  # pylint: disable=import-outside-toplevel
        )
        from jobs.load_cap_hits import (
            run_cap_hits,  # pylint: disable=import-outside-toplevel
        )
    except ModuleNotFoundError as exc:
        print("✅ Validation passed.")
        print("⚠️ Jobs not wired yet:", exc)
        print(
            "Create jobs/build_legacy_corsi.py, "
            "jobs/build_modern_player_game_es.py, jobs/load_cap_hits.py"
        )
        return 0

    run_legacy_corsi()
    run_modern_es()
    run_cap_hits()
    return 0


if __name__ == "__main__":
    sys.exit(main())
