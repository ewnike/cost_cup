"""
cluster_player_archetypes_modern.py.

Train archetype clusters on modern seasons (train years only), then predict clusters
for later seasons using the same scaler + KMeans model.

Supports running separately for Forwards (F) and Defensemen (D), writing into
position-specific mart tables:

  - mart.player_season_clusters_modern_truth_f / _d
  - mart.player_cluster_centers_modern_truth_f / _d

Usage:
  python cluster_player_archetypes_modern.py --position F
  python cluster_player_archetypes_modern.py --position D
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from db_utils import get_db_engine
from log_utils import setup_logger

logger = setup_logger()

# NOTE:
# This is the model feature list/order used to build X.
# It does NOT have to match the DB table column order as long as we reorder centers_df
# before writing.
FEATURES = [
    "toi_per_game",
    "es_share",
    "g60",
    "a60",
    "p60",
    "s60",
    "hit60",
    "blk60",
    "take60",
    "give60",
    "penl60",
    "fo_win_pct",
    "cf60",
    "ca60",
    "cf_percent",
]

# DB centers table column order (matches your mart.player_cluster_centers_modern_truth schema)
CENTERS_COLS = [
    "g60",
    "a60",
    "p60",
    "s60",
    "hit60",
    "blk60",
    "take60",
    "give60",
    "penl60",
    "fo_win_pct",
    "toi_per_game",
    "es_share",
    "cf60",
    "ca60",
    "cf_percent",
]

TRAIN_SEASONS = {20182019, 20192020, 20202021, 20212022, 20222023}
PREDICT_SEASONS = {20232024, 20242025}

K_FIXED = 3


SRC_SQL = """
SELECT
  f.*,
  p.pos
FROM mart.player_season_archetype_features_modern_truth_clean f
JOIN mart.player_season_pos_modern p
  ON p.season = f.season
 AND p.player_id = f.player_id
WHERE p.pos = :pos
"""


def run_for_position(pos: str) -> None:
    """Run clustering for one position group ('F' or 'D') and write outputs."""
    pos = pos.upper()
    if pos not in {"F", "D"}:
        raise ValueError("pos must be 'F' or 'D'")

    # output tables by position
    clusters_tbl = f"player_season_clusters_modern_truth_{pos.lower()}"
    centers_tbl = f"player_cluster_centers_modern_truth_{pos.lower()}"

    logger.info("Clustering position=%s", pos)
    logger.info("Writing to mart.%s and mart.%s", clusters_tbl, centers_tbl)

    # ---- read ----
    engine = get_db_engine()
    try:
        df = pd.read_sql_query(text(SRC_SQL), engine, params={"pos": pos})
        logger.info(
            "loaded rows=%s players=%s seasons=[%s..%s]",
            len(df),
            df["player_id"].nunique() if not df.empty else 0,
            df["season"].min() if not df.empty else None,
            df["season"].max() if not df.empty else None,
        )
    finally:
        engine.dispose()

    if df.empty:
        raise RuntimeError(
            f"No rows returned for pos={pos}. Check mart.player_season_pos_modern and joins."
        )

    # Fill faceoff win% for players with no draws
    if "fo_win_pct" in df.columns:
        df["fo_win_pct"] = df["fo_win_pct"].fillna(0.5)

    # Drop rows with missing model features
    df_model = df.dropna(subset=FEATURES).copy()

    # Split train/predict
    df_train = df_model[df_model["season"].isin(TRAIN_SEASONS)].copy()
    df_pred = df_model[df_model["season"].isin(PREDICT_SEASONS)].copy()

    if df_train.empty:
        raise RuntimeError(
            f"Train set is empty for pos={pos}. Check TRAIN_SEASONS and data."
        )

    # ---- fit on train only ----
    scaler = StandardScaler()
    X_train = df_train[FEATURES].to_numpy(dtype=float)
    Xs_train = scaler.fit_transform(X_train)

    k = K_FIXED
    km = KMeans(n_clusters=k, n_init=50, random_state=42)
    df_train["cluster"] = km.fit_predict(Xs_train)
    logger.info("Using fixed k=%s (trained on seasons=%s)", k, sorted(TRAIN_SEASONS))

    # ---- predict for later seasons ----
    if not df_pred.empty:
        X_pred = df_pred[FEATURES].to_numpy(dtype=float)
        Xs_pred = scaler.transform(X_pred)
        df_pred["cluster"] = km.predict(Xs_pred)

    # Reassemble
    df_out = pd.concat([df_train, df_pred], ignore_index=True)

    # clusters output
    out_clusters = df_out[
        ["season", "player_id", "team_id", "games_played", "toi_total_sec", "cluster"]
    ].copy()

    # centers output (inverse-transform centers back to original units)
    centers = scaler.inverse_transform(km.cluster_centers_)
    centers_df = pd.DataFrame(centers, columns=FEATURES)

    # reorder for DB schema
    centers_df = centers_df[CENTERS_COLS].copy()
    centers_df["cluster"] = np.arange(k, dtype=np.int64)

    expected = CENTERS_COLS + ["cluster"]
    assert list(centers_df.columns) == expected, centers_df.columns

    # ---- write ----
    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE mart.{clusters_tbl};"))
        out_clusters.to_sql(
            clusters_tbl,
            engine,
            schema="mart",
            if_exists="append",
            index=False,
            method="multi",
        )

        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE mart.{centers_tbl};"))
        centers_df.to_sql(
            centers_tbl,
            engine,
            schema="mart",
            if_exists="append",
            index=False,
            method="multi",
        )
    finally:
        engine.dispose()
        logger.info("Wrote mart.%s and mart.%s", clusters_tbl, centers_tbl)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Cluster player archetypes for F or D (modern seasons)."
    )
    parser.add_argument(
        "--position",
        dest="pos",
        choices=["F", "D", "f", "d"],
        required=True,
        help="Specify the position group: 'F' for forwards, 'D' for defensemen.",
    )
    args = parser.parse_args()
    run_for_position(args.pos)


if __name__ == "__main__":
    main()
