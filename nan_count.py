import pandas as pd
import zipfile
import os

# Define the paths
zip_file_path = "data/download/game_plays_players.zip"  # Update if needed
csv_filename = "game_plays_players.csv"  # Adjust if different

# Read CSV from ZIP without extracting
with zipfile.ZipFile(zip_file_path, "r") as z:
    with z.open(csv_filename) as f:
        df = pd.read_csv(f)

# Count NaNs per column
nan_counts = df.isna().sum()

# Get total rows with at least one NaN
rows_with_nans = df[df.isna().any(axis=1)]

# Get total rows
num_rows = df.shape[0]


# Print the results
print("NaN Counts Per Column:\n", nan_counts)
print(f"\nTotal Rows with NaNs: {rows_with_nans.shape[0]}")
print("\nSample Rows with NaNs:\n", rows_with_nans.head())
print(f"Total Rows: {num_rows}")
