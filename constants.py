"""
July 5 2025.

Make a file for global constants.

Eric Winiecke.
"""

import os

from dotenv import load_dotenv

load_dotenv()


# List of NHL seasons to be processed
SEASONS = [
    "20152016",
    "20162017",
    "20172018",
    "20182019",
    "20192020",
    "20202021",
    "20212022",
    "20222023",
    "20232024",
    "20242025",
    "20252026",
]

# ✅ Local Paths
local_download_path = os.getenv("LOCAL_DOWNLOAD_PATH", "data/download")
local_extract_path = os.getenv("LOCAL_EXTRACT_PATH", "data/extracted")

local_download_path_II = os.getenv("LOCAL_DOWNLOAD_PATH_II", "data_pbp_raw/download")
local_extract_path_II = os.getenv("LOCAL_EXTRACT_PATH_II", "data_pbp_raw/extracted")

local_download_path_III = os.getenv("LOCAL_DOWNLOAD_PATH_III", "data_shifts_raw/download")
local_extract_path_III = os.getenv("LOCAL_EXTRACT_PATH_III", "data_shifts_raw/extracted")

# ✅ S3 bucket name (global)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET_NAME:
    raise EnvironmentError("S3_BUCKET_NAME is not set in environment variables.")
