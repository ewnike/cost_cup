"""
August 11, 2024
main.py code needed for setting up db
and running 4 scripts that read in NHL
data from AWS S3 Buckets.
Eric Winiecke.
"""

import logging
import subprocess

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


def run_script(script_name):
    """
    Code to kickoff downloading data from AWS S3 buckets and
    inserting data into datatables in the hockey_stats database.
    """
    try:
        result = subprocess.run(
            ["python", script_name], capture_output=True, text=True, check=True
        )
        logging.info(f"Output of {script_name}:\n{result.stdout}")
        if result.stderr:
            logging.error(f"Errors in {script_name}:\n{result.stderr}")
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        # This will catch the error if the subprocess fails and check=True is set
        logging.error(f"Script {script_name} failed with error: {e.stderr}")
        print(f"Script {script_name} failed with error: {e.stderr}")
    except Exception as e:
        logging.error(f"Failed to run {script_name}: {e}")
        print(f"Failed to run {script_name}: {e}")


def main():
    """The main event."""
    scripts = [
        "game_processor.py",
        "game_shifts_processor.py",
        "game_skater_stats_processor.py",
        "game_plays_processor.py",
        "player_info_processor.py",
    ]

    for script in scripts:
        run_script(script)


if __name__ == "__main__":
    main()
