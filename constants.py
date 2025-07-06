"""
July 5 2025.

Make a file for global constants.

Eric Winiecke.
"""

import os

# List of NHL seasons to be processed
SEASONS = ["20152016", "20162017", "20172018"]

# ✅ Local Paths
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")

# ✅ S3 bucket name (global)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET_NAME:
    raise EnvironmentError("S3_BUCKET_NAME is not set in environment variables.")
