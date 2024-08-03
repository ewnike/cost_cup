import subprocess
import logging

# Configure logging
logging.basicConfig(
    filename="data_processing.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


def run_script(script_name):
    try:
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        logging.info(f"Output of {script_name}:\n{result.stdout}")
        if result.stderr:
            logging.error(f"Errors in {script_name}:\n{result.stderr}")
        print(result.stdout)
        print(result.stderr)
    except Exception as e:
        logging.error(f"Failed to run {script_name}: {e}")
        print(f"Failed to run {script_name}: {e}")


def main():
    scripts = [
        "game_processor.py",
        "game_shifts_processor.py",
        "game_skater_stats_processor.py",
        "game_plays_processor.py",
    ]

    for script in scripts:
        run_script(script)


if __name__ == "__main__":
    main()
