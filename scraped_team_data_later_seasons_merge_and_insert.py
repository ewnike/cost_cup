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

import pandas as pd

from data_processing_utils import clear_dir_patterns
from db_utils import get_db_engine

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
    salary_path = os.path.join(SALARY_DIR, f"team_salary_{end_year - 1}.csv")  # 2018 -> 20182019

    if not os.path.exists(stats_path):
        print(f"Missing stats file: {stats_path} (skipping)")
        continue
    if not os.path.exists(salary_path):
        print(f"Missing salary file: {salary_path} (skipping)")
        continue

    stats = pd.read_csv(stats_path)
    salary = pd.read_csv(salary_path)

    # Ensure salary team codes match your Abbreviation codes
    salary["Team"] = salary["Team"].astype(str).str.strip().str.upper()
    stats["Abbreviation"] = stats["Abbreviation"].astype(str).str.strip().str.upper()

    # Convert Total_Cap to numeric
    salary["Total_Cap_num"] = salary["Total_Cap"].apply(money_to_float)

    merged = pd.merge(
        stats,
        salary[["Team", "Avg_Age", "Total_Cap", "Total_Cap_num"]],
        left_on="Abbreviation",
        right_on="Team",
        how="inner",
        suffixes=("", "_salary"),
    )

    # --- Sanity checks BEFORE writing ---
    stats_codes = set(stats["Abbreviation"].astype(str).str.strip().str.upper())
    salary_codes = set(salary["Team"].astype(str).str.strip().str.upper())

    unmatched_stats = sorted(stats_codes - salary_codes)  # in records, not in salary
    unmatched_salary = sorted(salary_codes - stats_codes)  # in salary, not in records

    print(f"{season_id}: unmatched in stats (Abbreviation not in salary.Team): {unmatched_stats}")
    print(f"{season_id}: unmatched in salary (Team not in stats.Abbreviation): {unmatched_salary}")

    # --- Write Table -------------------------
    merged["season"] = season_id

    merged = merged.rename(
        columns={
            "Team": "team_name",
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
    # Drop salary join key now (we used it)
    merged = merged.drop(columns=["Team_salary", "Avg_Age"], errors="ignore")

    merged = merged[
        ["team_name", "abbr", "team_id", "gp", "w", "l", "otl", "pts", "total_cap_raw", "total_cap"]
    ].copy()

    table_name = f"team_summary_{season_id}"

    merged.to_sql(table_name, engine, schema="mart", if_exists="replace", index=False)
    print(f"{season_id}: merged rows={len(merged)} -> {table_name}")

    # ✅ only clear files AFTER successful insert for this season
    clear_dir_patterns(RECORDS_DIR, [f"NHL_{end_year}_team_stats.csv"])
    clear_dir_patterns(SALARY_DIR, [f"team_salary_{end_year - 1}.csv"])


engine.dispose()
