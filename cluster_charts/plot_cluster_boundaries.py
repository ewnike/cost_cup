import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from db_utils import get_db_engine

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

SQL_DATA = """
SELECT
  f.season,
  f.player_id,
  f.team_id,
  {cols}
FROM mart.player_season_archetype_features_modern_truth_clean_{pos} f
JOIN mart.player_season_clusters_modern_truth_{pos} c
  USING (season, player_id, team_id)
WHERE f.season BETWEEN 20182019 AND 20242025;
"""

SQL_CENTERS = """
SELECT {cols}, cluster
FROM mart.player_cluster_centers_modern_truth_{pos}
ORDER BY cluster;
"""


def plot_boundaries(
    df_model: pd.DataFrame, centers: pd.DataFrame, title: str, out_png: str
) -> None:
    # Scale in original feature space (to match how KMeans “should” behave)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(df_model[FEATURES].to_numpy(float))

    # PCA to 2D for plotting boundaries
    pca = PCA(n_components=2, random_state=42)
    X2 = pca.fit_transform(Xs)

    # Transform centers through same scaler + PCA
    C_scaled = scaler.transform(centers[FEATURES].to_numpy(float))
    C2 = pca.transform(C_scaled)

    # Fit a *new* KMeans in PCA space for boundaries (visualization-only)
    # (Boundaries in PCA space will approximate the real ones.)
    k = len(centers)
    km2 = KMeans(n_clusters=k, n_init=50, random_state=42)
    km2.fit(X2)

    # Grid for boundary regions
    pad = 0.6
    x_min, x_max = X2[:, 0].min() - pad, X2[:, 0].max() + pad
    y_min, y_max = X2[:, 1].min() - pad, X2[:, 1].max() + pad
    xx, yy = np.meshgrid(
        np.linspace(x_min, x_max, 400),
        np.linspace(y_min, y_max, 400),
    )
    Z = km2.predict(np.c_[xx.ravel(), yy.ravel()]).reshape(xx.shape)

    fig, ax = plt.subplots(figsize=(10, 7))

    # Boundary regions (no explicit colors set; matplotlib chooses defaults)
    ax.contourf(xx, yy, Z, alpha=0.18)

    # Player-seasons scatter
    ax.scatter(X2[:, 0], X2[:, 1], s=8, alpha=0.25)

    # Centers (from DB) in PCA space
    ax.scatter(C2[:, 0], C2[:, 1], s=220, marker="X", edgecolor="black")

    # Label centers
    for i, (cx, cy) in enumerate(C2):
        ax.text(cx, cy, f" {i}", fontsize=11, weight="bold")

    ax.set_title(title)
    ax.set_xlabel("PC1 (scaled features)")
    ax.set_ylabel("PC2 (scaled features)")
    ax.grid(True, alpha=0.2)

    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    print(f"✅ wrote {out_png}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pos", choices=["F", "D"], required=True)
    ap.add_argument("--out", default=None, help="output png filename")
    args = ap.parse_args()

    pos = args.pos.lower()
    cols = ", ".join([f"f.{c}" for c in FEATURES])

    engine = get_db_engine()
    try:
        df = pd.read_sql_query(text(SQL_DATA.format(cols=cols, pos=pos)), engine)
        ccols = ", ".join([f"{c}" for c in FEATURES])
        centers = pd.read_sql_query(
            text(SQL_CENTERS.format(cols=ccols, pos=pos)), engine
        )
    finally:
        engine.dispose()

    # Basic safety: drop NaNs
    df_model = df.dropna(subset=FEATURES).copy()

    out_png = args.out or f"cluster_boundaries_{pos}.png"
    title = f"{args.pos} KMeans clusters — PCA boundaries + centers (visualization)"
    plot_boundaries(df_model, centers, title=title, out_png=out_png)


if __name__ == "__main__":
    main()
