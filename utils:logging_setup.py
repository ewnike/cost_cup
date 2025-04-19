import logging


def setup_logging(filename="data_processing.log"):
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
