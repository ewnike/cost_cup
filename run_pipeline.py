# pipeline.py
import sys
from scripts.validate_db_paths import main as validate_main


def main() -> int:
    rc = validate_main()
    if rc != 0:
        return rc

    # Lazy imports so pipeline runs even before jobs exist
    try:
        from jobs.build_legacy_corsi import run_legacy_corsi
        from jobs.build_modern_player_game_es import run_modern_es
        from jobs.load_cap_hits import run_cap_hits
    except ModuleNotFoundError as e:
        print("✅ Validation passed.")
        print("⚠️ Jobs not wired yet:", e)
        print(
            "Create jobs/build_legacy_corsi.py, jobs/build_modern_player_game_es.py, jobs/load_cap_hits.py"
        )
        return 0

    run_legacy_corsi()
    run_modern_es()
    run_cap_hits()
    return 0


if __name__ == "__main__":
    sys.exit(main())
