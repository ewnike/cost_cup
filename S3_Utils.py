import logging
import os

import boto3
import botocore

# Initialize the S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv("S3_BUCKET_NAME")  # Get the bucket name from environment variables


def download_from_s3(bucket, key, download_path):
    """
    Downloads a file from the specified S3 bucket.

    Args:
    - bucket: S3 bucket name
    - key: File key (path in the S3 bucket)
    - download_path: Local path to save the downloaded file

    Returns:
    - None

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
    # Make sure the environment variable S3_BUCKET_NAME is set
    # Set file key and download path manually or via environment variables
    s3_file_key = os.getenv("S3_FILE_KEY", "example_file.zip")
    local_file_path = os.getenv("LOCAL_FILE_PATH", "data/example_file.zip")

    # Download the file
    if bucket_name:
        download_from_s3(bucket_name, s3_file_key, local_file_path)
    else:
        logging.error("Bucket name not found. Please set the S3_BUCKET_NAME environment variable.")
