"""
Helper functions for log files.

Author: Eric Winiecke
Date: April 2025
"""

import logging


def setup_logging(filename="data_processing.log"):
    """Set up logging configuration with a specified log file."""
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
