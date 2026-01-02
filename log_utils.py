"""
Helper functions for log files.

Author: Eric Winiecke
Date: April 2025
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(log_file_path="logs/data_processing.log"):
    """Set up a logger with rotating file handler and console output."""
    log_path = Path(log_file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = RotatingFileHandler(str(log_path), maxBytes=5 * 1024 * 1024, backupCount=3)
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

    return logger
