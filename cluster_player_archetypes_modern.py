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


def main() -> None:
    engine = get_db_engine()
    try:
        df = pd.read_sql_query(
            text("SELECT * FROM mart.player_season_archetype_features_modern"),
            engine,
        )
    finally:
        engine.dispose()

    df = pd.read_sql_query(
        text("SELECT * FROM mart.player_season_archetype_features_modern"), engine
    )

    # Fill faceoff win% for players with no draws
    df["fo_win_pct"] = df["fo_win_pct"].fillna(0.5)

    # Now dropna on remaining features
    df_model = df.dropna(subset=FEATURES).copy()

    X = df_model[FEATURES].to_numpy(dtype=float)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    best_k, best_score = None, -1.0
    for k in range(3, 11):
        km = KMeans(n_clusters=k, n_init=25, random_state=42)
        labels = km.fit_predict(Xs)
        score = silhouette_score(Xs, labels)
        logger.info("k=%s silhouette=%.3f", k, score)
        if score > best_score:
            best_k, best_score = k, score

    logger.info("Best k=%s silhouette=%.3f", best_k, best_score)

    km = KMeans(n_clusters=best_k, n_init=50, random_state=42)
    df_model["cluster"] = km.fit_predict(Xs)

    # write clusters
    out_clusters = df_model[
        ["season", "player_id", "team_id", "games_played", "toi_total_sec", "cluster"]
    ].copy()
    engine = get_db_engine()
    try:
        out_clusters.to_sql(
            "player_season_clusters_modern",
            engine,
            schema="mart",
            if_exists="replace",
            index=False,
            method="multi",
        )
    finally:
        engine.dispose()

    # cluster centers in original units
    centers = scaler.inverse_transform(km.cluster_centers_)
    centers_df = pd.DataFrame(centers, columns=FEATURES)
    centers_df["cluster"] = np.arange(best_k)

    engine = get_db_engine()
    try:
        centers_df.to_sql(
            "player_cluster_centers_modern",
            engine,
            schema="mart",
            if_exists="replace",
            index=False,
            method="multi",
        )
    finally:
        engine.dispose()

    logger.info("Wrote mart.player_season_clusters_modern and mart.player_cluster_centers_modern")


if __name__ == "__main__":
    main()
