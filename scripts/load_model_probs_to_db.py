#!/usr/bin/env python3
"""
Load all transition model probability CSVs from model_out/ into Postgres.

Files expected:
  model_out/transition_probs_F_test_season_*.csv
  model_out/transition_probs_D_test_season_*.csv

Writes:
  mart.cluster_transition_model_probs_f
  mart.cluster_transition_model_probs_d

Upsert key:
  (season_t, player_id)
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db_utils import get_db_engine

DDL_F = """
CREATE TABLE IF NOT EXISTS mart.cluster_transition_model_probs_f (
  season_t   integer NOT NULL,
  player_id  bigint  NOT NULL,
  cluster_t  integer NOT NULL,
  p_to0      double precision NOT NULL,
  p_to1      double precision NOT NULL,
  p_to2      double precision NOT NULL,
  source_file text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (season_t, player_id)
);
"""

DDL_D = """
CREATE TABLE IF NOT EXISTS mart.cluster_transition_model_probs_d (
  season_t   integer NOT NULL,
  player_id  bigint  NOT NULL,
  cluster_t  integer NOT NULL,
  p_to0      double precision NOT NULL,
  p_to1      double precision NOT NULL,
  p_to2      double precision NOT NULL,
  source_file text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (season_t, player_id)
);
"""

UPSERT_SQL = """
INSERT INTO {table} (season_t, player_id, cluster_t, p_to0, p_to1, p_to2, source_file)
VALUES (:season_t, :player_id, :cluster_t, :p_to0, :p_to1, :p_to2, :source_file)
ON CONFLICT (season_t, player_id) DO UPDATE SET
  cluster_t   = EXCLUDED.cluster_t,
  p_to0       = EXCLUDED.p_to0,
  p_to1       = EXCLUDED.p_to1,
  p_to2       = EXCLUDED.p_to2,
  source_file = EXCLUDED.source_file,
  created_at  = now();
"""

ENSURE_COLS_F = """
ALTER TABLE mart.cluster_transition_model_probs_f
  ADD COLUMN IF NOT EXISTS source_file text;

ALTER TABLE mart.cluster_transition_model_probs_f
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
"""

ENSURE_COLS_D = """
ALTER TABLE mart.cluster_transition_model_probs_d
  ADD COLUMN IF NOT EXISTS source_file text;

ALTER TABLE mart.cluster_transition_model_probs_d
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
"""

MODEL_OUT = Path("model_out")

F_PATTERN = str(MODEL_OUT / "transition_probs_F_test_season_*.csv")
D_PATTERN = str(MODEL_OUT / "transition_probs_D_test_season_*.csv")

f_files = sorted(MODEL_OUT.glob(F_PATTERN))
d_files = sorted(MODEL_OUT.glob(D_PATTERN))


def _has_prob_cols(fp: Path) -> bool:
    header = fp.open().readline().strip().split(",")
    return {"p_to0", "p_to1", "p_to2"}.issubset(set(header))


f_files = [fp for fp in f_files if _has_prob_cols(fp)]
d_files = [fp for fp in d_files if _has_prob_cols(fp)]

NEED = {"player_id", "season_t", "cluster_t", "p_to0", "p_to1", "p_to2"}


def _read_many(files: list[str], pos_group: str) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()

    dfs: list[pd.DataFrame] = []
    for fp in sorted(files):
        df = pd.read_csv(fp)

        # enforce expected columns
        missing = NEED - set(df.columns)
        if missing:
            print(f"SKIP (missing {sorted(missing)}): {fp}")
            continue

        df = df[
            ["player_id", "season_t", "cluster_t", "p_to0", "p_to1", "p_to2"]
        ].copy()
        df["pos_group"] = pos_group
        df["source_file"] = os.path.basename(fp)
        dfs.append(df)

    if not dfs:
        print(f"No usable files for pos_group={pos_group}.")
        return pd.DataFrame()

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)

    # dtype guardrails
    out["player_id"] = out["player_id"].astype("int64")
    out["season_t"] = out["season_t"].astype("int64")
    out["cluster_t"] = out["cluster_t"].astype("int64")
    for c in ["p_to0", "p_to1", "p_to2"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")

    # drop any rows with NaN probs (should be none; but keep DB clean)
    out = out.dropna(subset=["p_to0", "p_to1", "p_to2"]).reset_index(drop=True)

    # optional: normalize probs if tiny float drift exists
    s = out["p_to0"] + out["p_to1"] + out["p_to2"]
    # if any row is way off, we keep it but you can choose to raise
    # Here: renormalize safely for minor drift
    m = s > 0
    out.loc[m, "p_to0"] = out.loc[m, "p_to0"] / s[m]
    out.loc[m, "p_to1"] = out.loc[m, "p_to1"] / s[m]
    out.loc[m, "p_to2"] = out.loc[m, "p_to2"] / s[m]

    return out


def _upsert_df(conn, df: pd.DataFrame, table: str) -> int:
    if df.empty:
        return 0

    rows = df.to_dict("records")
    chunk = 5000
    sql = text(UPSERT_SQL.format(table=table))

    for i in range(0, len(rows), chunk):
        conn.execute(sql, rows[i : i + chunk])

    return len(df)


def main():
    f_files = [Path(x) for x in glob.glob(F_PATTERN)]
    d_files = [Path(x) for x in glob.glob(D_PATTERN)]

    # ✅ Step 2: filter out legacy/bad CSVs (like 20182019 missing p_to*)
    f_files = [fp for fp in f_files if _has_prob_cols(fp)]
    d_files = [fp for fp in d_files if _has_prob_cols(fp)]

    df_f = _read_many(f_files, "F")
    df_d = _read_many(d_files, "D")

    print(f"Found F files: {len(f_files)}")
    for x in sorted(f_files)[:5]:
        print("  ", x)
    if len(f_files) > 5:
        print("  ...")

    print(f"Found D files: {len(d_files)}")
    for x in sorted(d_files)[:5]:
        print("  ", x)
    if len(d_files) > 5:
        print("  ...")

    print(
        "df_f rows:",
        len(df_f),
        "seasons:",
        df_f["season_t"].nunique() if not df_f.empty else 0,
    )
    print(
        "df_d rows:",
        len(df_d),
        "seasons:",
        df_d["season_t"].nunique() if not df_d.empty else 0,
    )

    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            # create/alter tables once
            conn.execute(text(DDL_F))
            conn.execute(text(DDL_D))

            # ensure columns once
            conn.execute(text(ENSURE_COLS_F))
            conn.execute(text(ENSURE_COLS_D))

            n_f = _upsert_df(conn, df_f, "mart.cluster_transition_model_probs_f")
            n_d = _upsert_df(conn, df_d, "mart.cluster_transition_model_probs_d")

        print(f"Upserted F rows: {n_f}")
        print(f"Upserted D rows: {n_d}")
    finally:
        engine.dispose()

    print(f"Upserted F rows: {n_f}")
    print(f"Upserted D rows: {n_d}")

    # quick sanity checks
    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            r1 = (
                conn.execute(
                    text(
                        """
                    SELECT COUNT(*) AS n, COUNT(DISTINCT season_t) AS n_seasons
                    FROM mart.cluster_transition_model_probs_f;
                    """
                    )
                )
                .mappings()
                .first()
            )
            r2 = (
                conn.execute(
                    text(
                        """
                    SELECT COUNT(*) AS n, COUNT(DISTINCT season_t) AS n_seasons
                    FROM mart.cluster_transition_model_probs_d;
                    """
                    )
                )
                .mappings()
                .first()
            )
        print("F table:", dict(r1))
        print("D table:", dict(r2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
