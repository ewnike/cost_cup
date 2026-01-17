"""
Merge team_records + team_salaries into mart.team_summary_{season}.

Replaces legacy merged_team_stats_{year} tables.

Eric Winiecke (updated)
"""

import os
import re

import pandas as pd

from constants import SCHEMA
from data_processing_utils import clear_dir_patterns
from db_utils import get_db_engine

RECORDS_DIR = "team_records"
SALARY_DIR = "team_salaries"


def parse_cap_to_int(val) -> int | None:
    """Convert '$70,403,880' -> 70403880; return None if missing."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    digits = re.sub(r"[^0-9]", "", str(val))
    return int(digits) if digits else None


def year_to_season_id(end_year: int) -> int:
    """2016 -> 20152016, 2017 -> 20162017, 2018 -> 20172018."""
    return int(f"{end_year - 1}{end_year}")


def main():
    """Bring it all together now."""
    engine = get_db_engine()

    # Legacy end-years for NHL_{end_year}_team_stats.csv
    season_years = [2016, 2017, 2018]

    for end_year in season_years:
        season_id = year_to_season_id(end_year)

        stats_path = os.path.join(RECORDS_DIR, f"NHL_{end_year}_team_stats.csv")
        salary_path = os.path.join(SALARY_DIR, f"team_salary_{end_year - 1}.csv")

        if not os.path.exists(stats_path):
            print(f"Missing stats file: {stats_path} — skipping {season_id}")
            continue
        if not os.path.exists(salary_path):
            print(f"Missing salary file: {salary_path} — skipping {season_id}")
            continue

        try:
            stats = pd.read_csv(stats_path)
            salary = pd.read_csv(salary_path)

            # Normalize headers
            stats.columns = [c.strip() for c in stats.columns]
            salary.columns = [c.strip() for c in salary.columns]

            # Normalize join keys
            stats["Abbreviation"] = stats["Abbreviation"].astype(str).str.strip().str.upper()
            salary["Team"] = salary["Team"].astype(str).str.strip().str.upper()

            merged = pd.merge(
                stats,
                salary,
                left_on="Abbreviation",
                right_on="Team",
                how="inner",
                suffixes=("", "_salary"),
            )

            # Build final standardized output
            out = pd.DataFrame(
                {
                    "season": season_id,
                    # stats side team name is usually "Team"
                    "team_name": merged["Team"],
                    "abbr": merged["Abbreviation"],
                    "team_id": merged["Team_ID"].astype("int64"),
                    "gp": merged["GP"].astype("int64"),
                    "w": merged["W"].astype("int64"),
                    "l": merged["L"].astype("int64"),
                    "otl": merged["OTL"].astype("int64"),
                    "pts": merged["PTS"].astype("int64"),
                    "total_cap_raw": merged.get("Total_Cap"),
                }
            )

            out["total_cap"] = out["total_cap_raw"].apply(parse_cap_to_int).astype("Int64")

            table_name = f"team_summary_{season_id}"
            out.to_sql(
                table_name,
                engine,
                schema=SCHEMA["mart"],  # "mart"
                if_exists="replace",
                index=False,
            )

            print(f"✅ {season_id}: rows={len(out)} -> {SCHEMA['mart']}.{table_name}")

            # ✅ ONLY clear after successful insert for that season
            clear_dir_patterns(RECORDS_DIR, [os.path.basename(stats_path)])
            clear_dir_patterns(SALARY_DIR, [os.path.basename(salary_path)])

        except Exception as e:
            print(f"❌ Failed season {season_id} (end_year={end_year}): {e}")
            # ✅ do NOT delete files if anything failed
            continue

    engine.dispose()


if __name__ == "__main__":
    main()
