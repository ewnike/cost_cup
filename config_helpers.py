"""
Helper function.

Author: Eric Winiecke
Date: May 5, 2025

"""


# pylint: disable=too-many-arguments
def build_processing_config(
    *,
    bucket_name,
    s3_file_key,
    local_zip_path,
    local_extract_path,
    expected_csv_filename,
    table_definition_function,
    table_name,
    column_mapping,
    engine,
    local_download_path,
):
    """
    Build a standardized config dictionary for S3 extraction and data processing.

    Args:
    ----
        bucket_name (str): Name of the S3 bucket.
        s3_file_key (str): Key to the ZIP file in the S3 bucket.
        local_zip_path (str): Local path to download the ZIP file.
        local_extract_path (str): Local path to extract the CSV contents.
        expected_csv_filename (str): Name of the expected CSV file inside the ZIP.
        table_definition_function (Callable): SQLAlchemy function to define the table.
        table_name (str): Name of the PostgreSQL table to insert into.
        column_mapping (dict): Column names and types for cleaning.
        engine (sqlalchemy.Engine): SQLAlchemy engine instance for database connection.
        local_download_path (str): Directory for downloading the ZIP file.

    Returns:
    -------
        dict: Config dictionary with all values needed for process_and_insert_data().

    """
    return {
        "bucket_name": bucket_name,
        "s3_file_key": s3_file_key,
        "local_zip_path": local_zip_path,
        "local_extract_path": local_extract_path,
        "expected_csv_filename": expected_csv_filename,
        "table_definition_function": table_definition_function,
        "table_name": table_name,
        "column_mapping": column_mapping,
        "engine": engine,
        "handle_zip": bool(local_zip_path),
        "local_download_path": local_download_path,
    }
