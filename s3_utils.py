"""
s3_utils.py.

This module provides functionality to download files from an AWS S3 bucket.
It initializes an S3 client and retrieves environment variables for the bucket name
and file paths.

Example Usage:
    Set the following environment variables before running the script:
        - S3_BUCKET_NAME: The name of the S3 bucket
        - S3_FILE_KEY: The key (path) of the file in S3
        - LOCAL_FILE_PATH: The local path where the file should be saved

    Run the script to download the specified file:
        ```python
        python s3_utils.py
        ```

Author: Eric Winiecke
Date: February 2025
"""

import logging
import os

import boto3
import botocore

# Initialize the S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv(
    "S3_BUCKET_NAME"
)  # Get the bucket name from environment variables


def download_from_s3(bucket: str, key: str, download_path: str) -> None:
    """
    Download a file from an S3 bucket and save it to a local path.

    Args:
    ----
        bucket (str): Name of the S3 bucket.
        key (str): S3 object key (file path inside the bucket).
        download_path (str): Local file path to save the downloaded file.

    Raises:
    ------
        botocore.exceptions.ClientError:
        If the file does not exist in S3 or another S3 error occurs.

    Logs:
    ------
        - INFO: When a download starts and completes.
        - ERROR: If the file is not found or another error occurs.

    """
    logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
    try:
        s3_client.download_file(bucket, key, download_path)
        logging.info(f"Downloaded {key} from S3 to {download_path}")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error downloading file from S3: {e}")
        if e.response["Error"]["Code"] == "404":
            logging.error(f"The object {key} does not exist in bucket {bucket}.")
        else:
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example usage:
    # Ensure the environment variable S3_BUCKET_NAME is set
    # Set file key and download path manually or via environment variables
    s3_file_key = os.getenv("S3_FILE_KEY", "example_file.zip")
    local_file_path = os.getenv("LOCAL_FILE_PATH", "data/example_file.zip")

    # Download the file if the bucket name is available
    if bucket_name:
        download_from_s3(bucket_name, s3_file_key, local_file_path)
    else:
        logging.error(
            "Bucket name not found. Please set the S3_BUCKET_NAME environment variable."
        )
