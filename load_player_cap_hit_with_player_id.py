"""
load_player_cap_hits_with_player_id.py.

Loads per-season cap hits into dim.player_cap_hit_{season}
and resolves player_id using dim.dim_player_name_unique / dim.player_info.

Expected CSV columns:
firstName, lastName, capHit, spotrac_url

"""

from __future__ import annotations

import glob
import os
import re

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from constants import SCHEMA
from db_utils import create_caphit_table, get_db_engine, get_metadata
from schema_utils import fqs  # quoted fq: "schema"."table"

CSV_GLOB = "player_cap_hits/player_cap_hits_*.csv"

# if your file names are player_cap_hits_2018.csv -> season 20182019, etc.
YEAR_TO_SEASON = {
    2015: 20152016,
    2016: 20162017,
    2017: 20172018,
    2018: 20182019,
    2019: 20192020,
    2020: 20202021,
    2021: 20212022,
    2022: 20222023,
    2023: 20232024,
    2024: 20242025,
}


def py_norm_name(s: str) -> str:
    """Mirror your norm_name logic (good enough for cap-hit matching)."""
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"\b([od])['’]", r"\1", s)  # o'regan -> oregan
    s = re.sub(r"[.\-’']", " ", s)  # punctuation -> space
    s = re.sub(r"[^a-z ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    return s


def season_from_filename(path: str) -> int:
    year = int(os.path.basename(path).split("_")[-1].split(".")[0])
    return YEAR_TO_SEASON[year]


def main() -> None:
    engine = get_db_engine()
    md = get_metadata()
    Session = sessionmaker(bind=engine)

    files = sorted(glob.glob(CSV_GLOB))
    if not files:
        raise FileNotFoundError(f"No files matched {CSV_GLOB}")

    with Session() as session:
        for path in files:
            season = season_from_filename(path)
            table_name = f"player_cap_hit_{season}"
            table = create_caphit_table(
                f"{SCHEMA['dim']}.{table_name}", md
            )  # ok to pass schema-qualified name
            md.create_all(engine)

            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]
            df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
            if "spotrac_url" not in df.columns:
                df["spotrac_url"] = None

            # build name_key for matching
            df["name_key"] = (df["firstName"].fillna("") + " " + df["lastName"].fillna("")).map(
                py_norm_name
            )

            # pull player_id matches from dim tables
            dim_player_unique = fqs("dim", "dim_player_name_unique")
            dim_player_info = fqs("dim", "player_info")  # you have player_info in dim

            # (1) name_key -> player_id
            name_key_rows = df["name_key"].dropna().unique().tolist()
            if name_key_rows:
                q1 = text(f"""
                    SELECT name_key, player_id
                    FROM {dim_player_unique}
                    WHERE name_key = ANY(:keys)
                """)
                m1 = session.execute(q1, {"keys": name_key_rows}).fetchall()
                map1 = {r[0]: int(r[1]) for r in m1 if r[1] is not None}
                df["player_id"] = df["name_key"].map(map1)

            # (2) fallback exact name match to player_info
            missing = df["player_id"].isna()
            if missing.any():
                q2 = text(f"""
                    SELECT "firstName", "lastName", player_id
                    FROM {dim_player_info}
                """)
                pi = pd.DataFrame(
                    session.execute(q2).fetchall(), columns=["firstName", "lastName", "player_id"]
                )
                pi["firstName"] = pi["firstName"].astype(str)
                pi["lastName"] = pi["lastName"].astype(str)
                df = df.merge(pi, on=["firstName", "lastName"], how="left", suffixes=("", "_pi"))
                df["player_id"] = df["player_id"].fillna(df["player_id_pi"])
                df = df.drop(columns=["player_id_pi"], errors="ignore")

            # final shape
            out = df[["player_id", "firstName", "lastName", "capHit", "spotrac_url"]].copy()
            out = out.drop_duplicates(subset=["spotrac_url"], keep="first")

            # insert
            out.to_sql(table_name, engine, schema=SCHEMA["dim"], if_exists="replace", index=False)
            print(f"✅ Loaded {len(out)} rows -> {SCHEMA['dim']}.{table_name}")

    engine.dispose()


if __name__ == "__main__":
    main()
