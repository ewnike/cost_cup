"""
August 12, 2024.
Helper function for users that
want to download initial data for
cost_of_cup directly to their postgresql d
database.
Eric Winiecke
"""

import os

import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure AWS credentials
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

# Define bucket and file details
bucket_name = os.getenv("S3_BUCKET_NAME")
file_name = "hockey_stats_db.sql"
local_file_name = "your_local_file_name"

s3_file_key = os.getenv("S3_FILE_KEY", "hockey_stats_db.sql")

# Download the file
s3.download_file(bucket_name, file_name, local_file_name)

print(f"{file_name} has been downloaded as {local_file_name}")
