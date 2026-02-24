"""Docstring for cluster_player_archetypes_modern."""

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from db_utils import get_db_engine
from log_utils import setup_logger

logger = setup_logger()

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


SRC_SQL = """
SELECT f.*
FROM mart.player_season_archetype_features_modern_truth_clean f
JOIN dim.player_info pi
  ON pi.player_id = f.player_id
WHERE pi."primaryPosition" <> 'G'
"""


def main() -> None:
    """Docstring for main."""
    engine = get_db_engine()
    try:
        df = pd.read_sql_query(text(SRC_SQL), engine)
        # logger.info("rows=%s players=%s", len(df), df["player_id"].nunique())
    finally:
        engine.dispose()

    # Fill faceoff win% for players with no draws
    df["fo_win_pct"] = df["fo_win_pct"].fillna(0.5)

    #    -- # Drop rows with missing model features --
    #     df_model = df.dropna(subset=FEATURES).copy()

    # X = df_model[FEATURES].to_numpy(dtype=float)
    # scaler = StandardScaler()
    # Xs = scaler.fit_transform(X)

    #     best_k, best_score = None, -1.0
    #     for k in range(3, 11):
    #         km = KMeans(n_clusters=k, n_init=25, random_state=42)
    #         labels = km.fit_predict(Xs)
    #         score = silhouette_score(Xs, labels)
    #         logger.info("k=%s silhouette=%.3f", k, score)
    #         if score > best_score:
    #             best_k, best_score = k, score

    #     logger.info("Best k=%s silhouette=%.3f", best_k, best_score)

    #     km = KMeans(n_clusters=best_k, n_init=50, random_state=42)
    #     ---df_model["cluster"] = km.fit_predict(Xs) ---
    # Drop rows with missing model features
    df_model = df.dropna(subset=FEATURES).copy()

    X = df_model[FEATURES].to_numpy(dtype=float)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    TRAIN_SEASONS = {20182019, 20192020, 20202021, 20212022, 20222023}
    PREDICT_SEASONS = {20232024, 20242025}
    k = 3
    km = KMeans(n_clusters=k, n_init=50, random_state=42)
    df_model["cluster"] = km.fit_predict(Xs)
    logger.info("Using fixed k=%s", k)

    df_train = df_model[df_model["season"].isin(TRAIN_SEASONS)].copy()
    df_pred = df_model[df_model["season"].isin(PREDICT_SEASONS)].copy()

    # scale on train only
    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(df_train[FEATURES].to_numpy(float))

    # fit KMeans on train only (fixed k)
    # km = KMeans(n_clusters=K_FIXED, n_init=50, random_state=42)
    # df_train["cluster"] = km.fit_predict(Xs_train)

    # predict on later seasons
    if not df_pred.empty:
        Xs_pred = scaler.transform(df_pred[FEATURES].to_numpy(float))
        df_pred["cluster"] = km.predict(Xs_pred)

    # reassemble clustered dataset
    df_model = pd.concat([df_train, df_pred], ignore_index=True)

    out_clusters = df_model[
        ["season", "player_id", "team_id", "games_played", "toi_total_sec", "cluster"]
    ].copy()

    engine = get_db_engine()
    try:
        # --- clusters ---
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE mart.player_season_clusters_modern_truth;"))

        out_clusters.to_sql(
            "player_season_clusters_modern_truth",
            engine,
            schema="mart",
            if_exists="append",  # <-- FIX
            index=False,
            method="multi",
        )

        # --- centers ---
        centers = scaler.inverse_transform(km.cluster_centers_)
        centers_df = pd.DataFrame(centers, columns=FEATURES)
        # centers_df["cluster"] = np.arange(best_k)
        centers_df["cluster"] = np.arange(k)

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE mart.player_cluster_centers_modern_truth;"))

        centers_df.to_sql(
            "player_cluster_centers_modern_truth",
            engine,
            schema="mart",
            if_exists="append",  # <-- FIX
            index=False,
            method="multi",
        )
    finally:
        engine.dispose()
        logger.info(
            "Wrote mart.player_season_clusters_modern_truth and "
            "mart.player_cluster_centers_modern_truth"
        )


if __name__ == "__main__":
    main()
