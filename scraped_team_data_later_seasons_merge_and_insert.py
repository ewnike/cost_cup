"""
Docstring for scraped_team_data_later_seasons_merge_and_insert.

Script to merge team_record and team salaries
by season and insert data into the database.
Then delete files in team_records and
team salaries.

Eric Winiecke
January 14, 2026.
"""

import os
import pathlib

import pandas as pd

from data_processing_utils import clear_dir_patterns
from db_utils import get_db_engine

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

RECORDS_DIR = "team_records"
SALARY_DIR = "team_salaries"

# Hockey-Reference end years you scraped: NHL_2019..NHL_2025
end_years = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

engine = get_db_engine()


def money_to_float(x):
    """Convert cap_hit to float."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s or s == "-" or s.lower() == "n/a":
        return None
    s = s.replace("$", "").replace(",", "").replace('"', "")
    try:
        return float(s)
    except ValueError:
        return None


for end_year in end_years:
    season_id = int(f"{end_year - 1}{end_year}")  # 2019 -> 20182019

    stats_path = os.path.join(RECORDS_DIR, f"NHL_{end_year}_team_stats.csv")
    salary_path = os.path.join(SALARY_DIR, f"team_salary_{end_year - 1}.csv")

    if not os.path.exists(stats_path):
        print(f"Missing stats file: {stats_path} (skipping)")
        continue
    if not os.path.exists(salary_path):
        print(f"Missing salary file: {salary_path} (skipping)")
        continue

    stats = pd.read_csv(stats_path)
    salary = pd.read_csv(salary_path)

    # --- Normalize join keys (abbr codes) ---
    stats["Abbreviation"] = stats["Abbreviation"].astype(str).str.strip().str.upper()
    salary["Team"] = salary["Team"].astype(str).str.strip().str.upper()

    # --- Convert money field ---
    salary["Total_Cap_num"] = salary["Total_Cap"].apply(money_to_float)

    # --- Merge (stats.Team is full team name; salary.Team is abbreviation) ---
    merged = pd.merge(
        stats,
        salary[["Team", "Avg_Age", "Total_Cap", "Total_Cap_num"]],
        left_on="Abbreviation",
        right_on="Team",
        how="inner",
        suffixes=("", "_salary"),
    )

    # --- Sanity checks BEFORE writing ---
    stats_codes = set(stats["Abbreviation"].dropna().astype(str).str.strip().str.upper())
    salary_codes = set(salary["Team"].dropna().astype(str).str.strip().str.upper())

    unmatched_stats = sorted(stats_codes - salary_codes)
    unmatched_salary = sorted(salary_codes - stats_codes)

    print(f"{season_id}: unmatched in stats (Abbreviation not in salary.Team): {unmatched_stats}")
    print(f"{season_id}: unmatched in salary (Team not in stats.Abbreviation): {unmatched_salary}")

    # --- Build output schema (mart.team_summary_{season}) ---
    merged["season"] = season_id

    # ✅ team_name: prefer full team name from stats side
    if "Team" in merged.columns and merged["Team"].notna().any():
        merged["team_name"] = merged["Team"]
    elif "Team_salary" in merged.columns and merged["Team_salary"].notna().any():
        merged["team_name"] = merged["Team_salary"]
    else:
        merged["team_name"] = None

    merged = merged.rename(
        columns={
            "Abbreviation": "abbr",
            "Team_ID": "team_id",
            "GP": "gp",
            "W": "w",
            "L": "l",
            "OTL": "otl",
            "PTS": "pts",
            "Total_Cap": "total_cap_raw",
            "Total_Cap_num": "total_cap",
        }
    )

    # drop join artifacts / unused cols
    merged = merged.drop(columns=["Team", "Team_salary", "Avg_Age"], errors="ignore")

    merged = merged[
        [
            "season",
            "team_name",
            "abbr",
            "team_id",
            "gp",
            "w",
            "l",
            "otl",
            "pts",
            "total_cap_raw",
            "total_cap",
        ]
    ].copy()

    table_name = f"team_summary_{season_id}"

    try:
        merged.to_sql(table_name, engine, schema="mart", if_exists="replace", index=False)
        print(f"{season_id}: merged rows={len(merged)} -> mart.{table_name}")
    except Exception as e:
        print(f"{season_id}: FAILED writing mart.{table_name}: {e}")
        continue

    # ✅ only clear files AFTER successful insert for this season
    clear_dir_patterns(RECORDS_DIR, [os.path.basename(stats_path)])
    clear_dir_patterns(SALARY_DIR, [os.path.basename(salary_path)])


engine.dispose()
