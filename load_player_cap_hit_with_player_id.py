"""
load_player_cap_hits_with_player_id.py.

Loads per-season cap hits into dim.player_cap_hit_{season}
and resolves player_id using:
  (0) dim.spotrac_to_nhl (spotrac_url -> spotrac_id -> player_id)
  (1) dim.dim_player_name_unique (name_key -> player_id)
  (2) dim.player_info (normalized name_key -> player_id)

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
from data_processing_utils import clear_player_cap_hits_dir  # noqa: F401
from db_utils import create_caphit_table, get_db_engine, get_metadata  # noqa: F401
from schema_utils import fqs  # quoted fq: "schema"."table"

CSV_GLOB = "player_cap_hits/player_cap_hits_*.csv"

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

# Matches:
#   .../redirect/player/1837
#   .../player/1837
# and allows trailing / or end-of-string
_SPOTRAC_ID_RE = re.compile(r"/(?:redirect/)?player/(\d+)(?:/|$)")


def fetch_spotrac_map(session, spotrac_ids: list[int]) -> dict[int, int]:
    """Return {spotrac_id: player_id} from dim.spotrac_to_nhl for the provided ids."""
    if not spotrac_ids:
        return {}

    q = text(f"""
        SELECT spotrac_id, player_id
        FROM {SCHEMA["dim"]}.spotrac_to_nhl
        WHERE spotrac_id = ANY(:sids)
          AND player_id IS NOT NULL
    """)
    rows = session.execute(q, {"sids": spotrac_ids}).fetchall()
    return {int(sid): int(pid) for (sid, pid) in rows}


def spotrac_id_from_url(url: str | None) -> int | None:
    """
    Docstring for spotrac_id_from_url.

    :param url: Description
    :type url: str | None
    :return: Description
    :rtype: int | None
    """
    if not url:
        return None
    m = _SPOTRAC_ID_RE.search(str(url))
    return int(m.group(1)) if m else None


def py_norm_name(s: str) -> str:
    """Mirror your norm_name logic (good enough for cap-hit matching)."""
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"\b([od])['’]", r"\1", s)  # o'regan -> oregan
    s = re.sub(r"[.\-’']", " ", s)  # punctuation -> space
    s = re.sub(r"[^a-z ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # collapse initials like "p a" -> "pa"
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    return s


def season_from_filename(path: str) -> int:
    """Turn year into season."""
    year = int(os.path.basename(path).split("_")[-1].split(".")[0])
    return YEAR_TO_SEASON[year]


def main() -> None:
    """Docstring for main."""
    engine = get_db_engine()
    _md = get_metadata()  # noqa: F841
    Session = sessionmaker(bind=engine)

    files = sorted(glob.glob(CSV_GLOB))
    if not files:
        raise FileNotFoundError(f"No files matched {CSV_GLOB}")

    loaded_files: list[str] = []

    try:
        with Session() as session:
            for path in files:
                if not os.path.exists(path):
                    print(f"⚠️ Missing file (skipping): {path}")
                    continue

                season = season_from_filename(path)
                table_name = f"player_cap_hit_{season}"

                # Optional: ensure table exists (if your create_caphit_table supports season)
                # create_caphit_table(engine, season=season)

                # --- READ CSV ---
                df = pd.read_csv(path)
                df.columns = [c.strip() for c in df.columns]
                df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
                if "spotrac_url" not in df.columns:
                    df["spotrac_url"] = None

                # Parse spotrac_id once
                df["spotrac_id"] = df["spotrac_url"].map(spotrac_id_from_url)

                # Build name_key and initialize player_id
                df["name_key"] = (df["firstName"].fillna("") + " " + df["lastName"].fillna("")).map(
                    py_norm_name
                )
                df["player_id"] = pd.NA

                # (0) Prefer deterministic mapping via dim.spotrac_to_nhl
                spotrac_ids_list = (
                    df.loc[df["spotrac_id"].notna(), "spotrac_id"]
                    .astype("int64")
                    .drop_duplicates()
                    .tolist()
                )

                sid_to_pid: dict[int, int] = {}
                if spotrac_ids_list:
                    sid_to_pid = fetch_spotrac_map(session, spotrac_ids_list)

                has_sid = df["spotrac_id"].notna()
                df.loc[has_sid, "player_id"] = df.loc[has_sid, "spotrac_id"].map(sid_to_pid)
                print("unresolved rows after spotrac_to_nhl:", int(df["player_id"].isna().sum()))
                print("spotrac_to_nhl-mapped rows:", int(df["player_id"].notna().sum()))
                print("unresolved rows after spotrac_to_nhl:", int(df["player_id"].isna().sum()))

                # (1) dim_player_name_unique
                dim_player_unique = fqs("dim", "dim_player_name_unique")
                dim_player_info = fqs("dim", "player_info")

                name_key_rows = df["name_key"].dropna().unique().tolist()
                if name_key_rows:
                    q1 = text(f"""
                        SELECT name_key, player_id
                        FROM {dim_player_unique}
                        WHERE name_key = ANY(:keys)
                    """)
                    m1 = session.execute(q1, {"keys": name_key_rows}).fetchall()
                    map1 = {r[0]: int(r[1]) for r in m1 if r[1] is not None}

                    missing = df["player_id"].isna()
                    df.loc[missing, "player_id"] = df.loc[missing, "name_key"].map(map1)
                    print(
                        "unresolved rows after dim_player_name_unique:",
                        int(df["player_id"].isna().sum()),
                    )
                # (2) fallback: normalize dim.player_info on the fly
                missing = df["player_id"].isna()
                if missing.any():
                    q = text(f'SELECT player_id, "firstName", "lastName" FROM {dim_player_info}')
                    pi = pd.DataFrame(
                        session.execute(q).fetchall(),
                        columns=["player_id", "firstName", "lastName"],
                    )

                    pi["name_key"] = (
                        pi["firstName"].fillna("") + " " + pi["lastName"].fillna("")
                    ).map(py_norm_name)
                    pi = pi.dropna(subset=["name_key"]).drop_duplicates(
                        subset=["name_key"], keep="first"
                    )

                    key_to_pid = dict(zip(pi["name_key"], pi["player_id"]))
                    df.loc[missing, "player_id"] = df.loc[missing, "name_key"].map(key_to_pid)
                    print(
                        "unresolved rows after dim_player_info fallback:",
                        int(df["player_id"].isna().sum()),
                    )
                # Output
                out = df[["player_id", "firstName", "lastName", "capHit", "spotrac_url"]].copy()
                out = out.drop_duplicates(subset=["spotrac_url"], keep="first")

                out.to_sql(
                    table_name,
                    engine,
                    schema=SCHEMA["dim"],
                    if_exists="replace",
                    index=False,
                )
                print(f"✅ Loaded {len(out)} rows -> {SCHEMA['dim']}.{table_name}")
                loaded_files.append(path)

        # delete only files that were successfully loaded
        for p in loaded_files:
            try:
                os.remove(p)
                print(f"🧹 deleted {p}")
            except OSError as e:
                print(f"⚠️ could not delete {p}: {e}")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
