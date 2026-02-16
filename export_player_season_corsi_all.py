"""Export player_season_corsi_all."""

import os
import pathlib
import re

import pandas as pd
from sqlalchemy import inspect, text

from db_utils import get_db_engine

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

RAW_SCHEMA = "derived"
OUT_CSV = "player_season_corsi_all.csv"
SEASON_RE = re.compile(r"^raw_corsi_(\d{8})$")


def list_raw_corsi_tables(engine) -> list[str]:
    insp = inspect(engine)
    tables = insp.get_table_names(schema=RAW_SCHEMA)
    return sorted([t for t in tables if t.startswith("raw_corsi_")])


def season_from_table(table_name: str) -> str | None:
    m = SEASON_RE.match(table_name)
    return m.group(1) if m else None


def export_player_season_corsi_all() -> None:
    engine = get_db_engine()
    try:
        tables = list_raw_corsi_tables(engine)
        season_tables = [(season_from_table(t), t) for t in tables]
        season_tables = [(s, t) for s, t in season_tables if s is not None]

        if not season_tables:
            raise RuntimeError(f"No tables matched raw_corsi_######## in schema '{RAW_SCHEMA}'.")

        out_rows: list[pd.DataFrame] = []

        with engine.connect() as conn:
            for season, table in season_tables:
                print(f"Aggregating {RAW_SCHEMA}.{table} -> season {season}")

                q = text(f"""
                    SELECT
                        :season AS season,
                        player_id::bigint AS player_id,
                        SUM(corsi_for)::double precision AS corsi_for,
                        SUM(corsi_against)::double precision AS corsi_against
                    FROM {RAW_SCHEMA}.{table}
                    GROUP BY player_id
                    ORDER BY player_id
                """)

                df = pd.read_sql_query(q, conn, params={"season": season})

                # Recompute cf_percent robustly
                denom = df["corsi_for"] + df["corsi_against"]
                df["cf_percent"] = (df["corsi_for"] / denom).fillna(0.0)

                out_rows.append(df)

        all_df = pd.concat(out_rows, ignore_index=True)
        all_df = all_df.sort_values(["season", "player_id"], ignore_index=True)

        all_df.to_csv(OUT_CSV, index=False)
        print(f"Wrote {OUT_CSV} rows={len(all_df)} seasons={all_df['season'].nunique()}")

    finally:
        engine.dispose()


if __name__ == "__main__":
    export_player_season_corsi_all()
