"""
Merge team_records + team_salaries into mart.team_summary_{season}.

Replaces legacy merged_team_stats_{year} tables.

Eric Winiecke (updated)
"""

import os
import re

import pandas as pd

# If you have SCHEMA in constants.py
from constants import SCHEMA
from data_processing_utils import clear_dir_patterns
from db_utils import get_db_engine


def parse_cap_to_int(val) -> int | None:
    """Convert '$70,403,880' -> 70403880; return None if missing."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val)
    digits = re.sub(r"[^0-9]", "", s)
    return int(digits) if digits else None


def year_to_season_id(year_str: str) -> int:
    """
    2016 -> 20152016
    2017 -> 20162017
    2018 -> 20172018.
    """  # noqa: D205
    y = int(year_str)
    return int(f"{y - 1}{y}")


def main():
    engine = get_db_engine()

    # Your original seasons (NHL_2016_team_stats.csv etc.)
    season_years = ["2016", "2017", "2018"]

    for year in season_years:
        season_id = year_to_season_id(year)

        stats_path = f"team_records/NHL_{int(year)}_team_stats.csv"
        salary_path = f"team_salaries/team_salary_{int(year) - 1}.csv"

        if not os.path.exists(stats_path):
            print(f"Missing stats file: {stats_path} — skipping {season_id}")
            continue
        if not os.path.exists(salary_path):
            print(f"Missing salary file: {salary_path} — skipping {season_id}")
            continue

        try:
            stats = pd.read_csv(stats_path)
            salary = pd.read_csv(salary_path)

            # Normalize headers (strip whitespace)
            stats.columns = [c.strip() for c in stats.columns]
            salary.columns = [c.strip() for c in salary.columns]

            # Expected inputs:
            # stats: Team, GP, W, L, OTL, PTS, Abbreviation, Team_ID
            # salary: Team, Avg_Age, Total_Cap
            merged = pd.merge(
                stats,
                salary,
                left_on="Abbreviation",
                right_on="Team",
                how="inner",
            )

            # Keep salary abbreviation, drop redundant salary Team col
            merged = merged.drop(columns=["Team_y"], errors="ignore")

            # Standardize output columns
            merged_out = pd.DataFrame(
                {
                    "season": season_id,
                    "team_name": merged.get("Team_x", merged.get("Team")),
                    "abbr": merged.get("Abbreviation", merged.get("Team")),
                    "team_id": merged["Team_ID"],
                    "gp": merged["GP"],
                    "w": merged["W"],
                    "l": merged["L"],
                    "otl": merged["OTL"],
                    "pts": merged["PTS"],
                    "total_cap_raw": merged.get("Total_Cap"),
                }
            )

            merged_out["total_cap"] = merged_out["total_cap_raw"].apply(parse_cap_to_int)

            # Write to mart schema
            out_table = f"team_summary_{season_id}"
            out_schema = SCHEMA["mart"]  # "mart"

            merged_out.to_sql(
                out_table,
                engine,
                schema=out_schema,
                if_exists="replace",
                index=False,
            )

            print(f"✅ Created {out_schema}.{out_table} rows={len(merged_out)}")

            # Delete inputs only after success
            os.remove(stats_path)
            os.remove(salary_path)
            print(f"🧹 Deleted {stats_path} and {salary_path}")

        except Exception as e:
            print(f"❌ Failed season {season_id} ({year}): {e}")

    engine.dispose()


if __name__ == "__main__":
    main()
