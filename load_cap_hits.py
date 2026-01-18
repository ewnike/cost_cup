"""
load_cap_hits.py.

Loads Spotrac cap hit CSVs into:
  1) dim.player_cap_hit_spotrac (historical by season_year + spotrac_url)
  2) dim.player_cap_hit_{season_id} (one table per NHL season, for joins)

CSV input files:
  player_cap_hits/player_cap_hits_2018.csv
  player_cap_hits/player_cap_hits_2019.csv
  ...

Rules:
- year in filename is the START year of the NHL season
  2018 -> season_id 20182019
- De-dupe by spotrac_url when possible.
"""

from __future__ import annotations

import glob
import os
from typing import Optional

import pandas as pd
from sqlalchemy import text

from constants import SCHEMA
from data_processing_utils import clear_dir_patterns
from db_utils import get_db_engine
from schema_utils import fq_q

CSV_GLOB = "player_cap_hits/player_cap_hits_*.csv"


def season_year_from_filename(path: str) -> int:
    """Derive player_cap_hits_2018.csv -> 2018."""
    base = os.path.basename(path)
    year_str = base.split("_")[-1].split(".")[0]
    return int(year_str)


def season_id_from_start_year(start_year: int) -> int:
    """Derive 2018 -> 20182019."""
    return int(f"{start_year}{start_year + 1}")


def clean_spotrac_url(v) -> Optional[str]:
    """Clean spotrac url."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def main() -> None:
    """Execute code when called."""
    engine = get_db_engine()
    files = sorted(glob.glob(CSV_GLOB))

    if not files:
        raise FileNotFoundError(f"No files matched {CSV_GLOB}")

    # Targets
    spotrac_table = fq_q("dim", "player_cap_hit_spotrac")  # dim.player_cap_hit_spotrac

    with engine.begin() as conn:
        for path in files:
            start_year = season_year_from_filename(path)  # 2018
            season_id = season_id_from_start_year(start_year)  # 20182019

            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]

            # Ensure expected columns exist
            for col in ["firstName", "lastName", "capHit"]:
                if col not in df.columns:
                    raise ValueError(f"{path} missing required column: {col}")

            if "spotrac_url" not in df.columns:
                # older files might not have it
                df["spotrac_url"] = None

            # Clean
            df["season_year"] = start_year
            df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
            df["spotrac_url"] = df["spotrac_url"].apply(clean_spotrac_url)

            # Drop rows missing core fields
            df = df.dropna(subset=["firstName", "lastName", "capHit"]).copy()

            # De-dupe: prefer rows with a real spotrac_url
            df["_has_url"] = df["spotrac_url"].notna().astype(int)
            df = (
                df.sort_values(["_has_url"], ascending=False)
                .drop_duplicates(subset=["firstName", "lastName"], keep="first")
                .drop(columns=["_has_url"])
                .reset_index(drop=True)
            )

            # --------
            # 1) Upsert into dim.player_cap_hit_spotrac (by season_year + spotrac_url)
            # Only upsert rows where URL exists; otherwise Spotrac table can't key correctly.
            df_spotrac = df[df["spotrac_url"].notna()].copy()

            if not df_spotrac.empty:
                records = df_spotrac[
                    ["season_year", "spotrac_url", "firstName", "lastName", "capHit"]
                ].to_dict("records")

                conn.execute(
                    text(f"""
                        INSERT INTO {spotrac_table}
                          (season_year, spotrac_url, "firstName", "lastName", "capHit")
                        VALUES
                          (:season_year, :spotrac_url, :firstName, :lastName, :capHit)
                        ON CONFLICT (season_year, spotrac_url) DO UPDATE
                        SET "firstName" = EXCLUDED."firstName",
                            "lastName"  = EXCLUDED."lastName",
                            "capHit"    = EXCLUDED."capHit";
                    """),
                    records,
                )

            # --------
            # 2) Replace dim.player_cap_hit_{season_id} (one table per season)
            # This table is used for joining into mart.aggregated_corsi_{season_id}, etc.
            season_table_name = f"player_cap_hit_{season_id}"
            season_table = fq_q("dim", season_table_name)

            # Make sure column set matches your dim.player_cap_hit_{season} schema
            df_season = df[["firstName", "lastName", "capHit", "spotrac_url"]].copy()

            # Replace per-season table (simple + deterministic)
            conn.execute(text(f"DROP TABLE IF EXISTS {season_table} CASCADE;"))
            conn.execute(
                text(f"""
                CREATE TABLE {season_table} (
                    "firstName"   varchar(50),
                    "lastName"    varchar(50),
                    "capHit"      double precision,
                    spotrac_url   text
                );
            """)
            )

            # Bulk insert
            conn.execute(
                text(f"""
                    INSERT INTO {season_table} ("firstName","lastName","capHit",spotrac_url)
                    VALUES (:firstName,:lastName,:capHit,:spotrac_url)
                """),
                df_season.to_dict("records"),
            )

            # Helpful index for joins
            conn.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS ix_{season_table_name}_name ON {season_table} ("firstName","lastName");'
                )
            )
            conn.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS ix_{season_table_name}_url  ON {season_table} (spotrac_url);"
                )
            )

            print(
                f"✅ {season_id}: season_rows={len(df_season)} "
                f"spotrac_rows_upserted={len(df_spotrac)} from {path}"
            )

            # Optional: clear the CSV after success
            # clear_dir_patterns("player_cap_hits", [os.path.basename(path)])

    engine.dispose()


if __name__ == "__main__":
    main()
