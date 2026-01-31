"""
cluster_skater_archetypes.

Pipeline:
- Load game-by-game skater stats from game_skater_stats.csv
- Derive season from game_id
- Aggregate to player-season
- Engineer per-60 scoring/checking/PK/PP features
- Scale features
- Run KMeans (k chosen by silhouette)
- Inspect cluster centers & example players
"""

# import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

# from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from stats_utils import add_corsi_rates_and_merge


# --------------------------------------------------
# 1. Load game-by-game skater stats
# --------------------------------------------------

csv_path = "game_skater_stats.csv"  # <-- change if needed
df_games = pd.read_csv(csv_path)

print("Columns:", df_games.columns.tolist())
print("Player-game rows:", len(df_games))


# --------------------------------------------------
# 2. Derive season from game_id
# --------------------------------------------------
# game_id like 2016020045 -> season "20162017"


def game_id_to_season(gid: int) -> str:
    """
    Transform game_id_to_season.

    :param gid: Description
    :type gid: int
    :return: Description
    :rtype: str
    """
    year_start = gid // 1_000_000
    return f"{year_start}{year_start + 1}"


df_games["season"] = df_games["game_id"].astype(int).apply(game_id_to_season)


# --------------------------------------------------
# 3. Aggregate to player-season
# --------------------------------------------------

group_cols = ["player_id", "season", "team_id"]

agg_dict = {
    "game_id": "nunique",  # games played
    "timeOnIce": "sum",
    "evenTimeOnIce": "sum",
    "powerPlayTimeOnIce": "sum",
    "shortHandedTimeOnIce": "sum",
    "goals": "sum",
    "assists": "sum",
    "shots": "sum",
    "hits": "sum",
    "powerPlayGoals": "sum",
    "powerPlayAssists": "sum",
    "shortHandedGoals": "sum",
    "shortHandedAssists": "sum",
    "penaltyMinutes": "sum",
    "faceOffWins": "sum",
    "faceoffTaken": "sum",
    "takeaways": "sum",
    "giveaways": "sum",
    "blocked": "sum",
    "plusMinus": "sum",
}

df_season = df_games.groupby(group_cols).agg(agg_dict).reset_index()
df_season = df_season.rename(columns={"game_id": "games_played"})

print("Player-season rows:", len(df_season))


# --------------------------------------------------
# 4. Filter low-TOI players
# --------------------------------------------------
# timeOnIce is seconds -> convert to minutes

df_season["toi_total_min"] = df_season["timeOnIce"] / 60.0
df_season["toi_ev_min"] = df_season["evenTimeOnIce"] / 60.0
df_season["toi_pp_min"] = df_season["powerPlayTimeOnIce"] / 60.0
df_season["toi_sh_min"] = df_season["shortHandedTimeOnIce"] / 60.0

MIN_TOI_MIN = 200  # tweak as you like
df_season = df_season[df_season["toi_total_min"] >= MIN_TOI_MIN].copy()
print("After TOI >= 200 minutes:", len(df_season), "player-seasons")


# --------------------------------------------------
# 5. Feature engineering (per-60 & usage)
# --------------------------------------------------

df_season = df_season[df_season["toi_total_min"] > 0].copy()

# Scoring & shot rates
df_season["G60"] = df_season["goals"] / df_season["toi_total_min"] * 60.0
df_season["A60"] = df_season["assists"] / df_season["toi_total_min"] * 60.0
df_season["P60"] = (df_season["goals"] + df_season["assists"]) / df_season["toi_total_min"] * 60.0
df_season["S60"] = df_season["shots"] / df_season["toi_total_min"] * 60.0

# Physical / defensive stats
df_season["HIT60"] = df_season["hits"] / df_season["toi_total_min"] * 60.0
df_season["BLK60"] = df_season["blocked"] / df_season["toi_total_min"] * 60.0
df_season["TAKE60"] = df_season["takeaways"] / df_season["toi_total_min"] * 60.0
df_season["GIVE60"] = df_season["giveaways"] / df_season["toi_total_min"] * 60.0
df_season["PIM60"] = df_season["penaltyMinutes"] / df_season["toi_total_min"] * 60.0

# Special teams
df_season["PP_PTS60"] = (
    (df_season["powerPlayGoals"] + df_season["powerPlayAssists"])
    / df_season["toi_total_min"]
    * 60.0
)
df_season["SH_PTS60"] = (
    (df_season["shortHandedGoals"] + df_season["shortHandedAssists"])
    / df_season["toi_total_min"]
    * 60.0
)

# Usage by situation (minutes per game)
df_season["EV_TOI_per_game"] = df_season["toi_ev_min"] / df_season["games_played"]
df_season["PP_TOI_per_game"] = df_season["toi_pp_min"] / df_season["games_played"]
df_season["SH_TOI_per_game"] = df_season["toi_sh_min"] / df_season["games_played"]
df_season["TOI_per_game"] = df_season["toi_total_min"] / df_season["games_played"]

# Faceoff win %
df_season["fo_attempts"] = df_season["faceoffTaken"]
df_season["fo_win_pct"] = np.where(
    df_season["fo_attempts"] > 0,
    df_season["faceOffWins"] / df_season["fo_attempts"],
    np.nan,
)
df_season["fo_win_pct"] = df_season["fo_win_pct"].fillna(df_season["fo_win_pct"].mean())

# plusMinus per 60 just as another flavor
df_season["PLUSMINUS60"] = df_season["plusMinus"] / df_season["toi_total_min"] * 60.0


# --------------------------------------------------
# 6. Select features for clustering
# --------------------------------------------------

# 2. --- CORSI MERGE BLOCK GOES HERE ---
df_corsi = pd.read_csv("player_season_corsi_2015_2018.csv")

# make sure 'season' matches df_season (both as string)
df_corsi["season"] = df_corsi["season"].astype(str)
df_season["season"] = df_season["season"].astype(str)

df_corsi["toi_corsi_min"] = df_corsi["time_on_ice"] / 60.0
df_corsi = df_corsi[df_corsi["toi_corsi_min"] > 0].copy()

df_corsi["CF60"] = df_corsi["corsi_for"] / df_corsi["toi_corsi_min"] * 60.0
df_corsi["CA60"] = df_corsi["corsi_against"] / df_corsi["toi_corsi_min"] * 60.0
df_corsi["CF_pct"] = df_corsi["cf_percent"]

df_merged = add_corsi_rates_and_merge(df_season, df_corsi)
# df_merged = df_season.merge(
#     df_corsi[["player_id", "season", "CF60", "CA60", "CF_pct", "cap_hit"]],
#     on=["player_id", "season"],
#     how="left",
# )

# 3. use df_merged for clustering, not df_season
features = [
    # existing features...
    "G60",
    "A60",
    "P60",
    "S60",
    "HIT60",
    "BLK60",
    "TAKE60",
    "GIVE60",
    "PIM60",
    "PP_PTS60",
    "SH_PTS60",
    "EV_TOI_per_game",
    "PP_TOI_per_game",
    "SH_TOI_per_game",
    "TOI_per_game",
    "fo_win_pct",
    "PLUSMINUS60",
    # new Corsi features
    "CF60",
    "CA60",
    "CF_pct",
    # optional:
    "cap_hit",
]

# keep only seasons where we actually have Corsi
target_seasons = ["20152016", "20162017", "20172018"]
df_model = df_merged[df_merged["season"].isin(target_seasons)].copy()

# drop rows with any NaN in the features
df_model = df_model.dropna(subset=features)

X = df_model[features].values


# --------------------------------------------------
# 7. Scale features
# --------------------------------------------------

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


# --------------------------------------------------
# 8. Try different k and choose by silhouette
# --------------------------------------------------

best_k = None
best_score = -1.0

for k in range(3, 11):  # 3–10 clusters
    km = KMeans(n_clusters=k, n_init=25, random_state=42)
    labels = km.fit_predict(X_scaled)
    score = silhouette_score(X_scaled, labels)
    print(f"k={k}, silhouette={score:.3f}")
    if score > best_score:
        best_score = score
        best_k = k

print(f"\nBest k = {best_k} (silhouette={best_score:.3f})")


# --------------------------------------------------
# 9. Fit final model & assign clusters
# --------------------------------------------------

# k_final = best_k
# kmeans_final = KMeans(n_clusters=k_final, n_init=50, random_state=42)
# df_season["cluster"] = kmeans_final.fit_predict(X_scaled)

k_final = best_k
kmeans_final = KMeans(n_clusters=k_final, n_init=50, random_state=42)
df_model["cluster"] = kmeans_final.fit_predict(X_scaled)

# --------------------------------------------------
# 10. Inspect cluster centers in original units
# --------------------------------------------------

centers_scaled = kmeans_final.cluster_centers_
centers = scaler.inverse_transform(centers_scaled)

cluster_centers = pd.DataFrame(centers, columns=features)
cluster_centers["cluster"] = range(k_final)
cluster_centers = cluster_centers.set_index("cluster")

print("\nCluster centers (original units, rounded):")
print(cluster_centers.round(2))


# --------------------------------------------------
# 11. Sample players per cluster
# --------------------------------------------------

display_cols = (
    [
        "player_id",
        "season",
        "team_id",
        "games_played",
        "toi_total_min",
    ]
    + features
    + ["cluster"]
)

for c in range(k_final):
    print(f"\n=== Cluster {c} example players ===")
    subset = df_model[df_model["cluster"] == c]
    if subset.empty:
        print("  (no players)")
        continue
    sample = subset.sample(n=min(10, len(subset)), random_state=42)
    print(sample[display_cols].round(2).to_string(index=False))


# --------------------------------------------------
# 12. Optional: PCA 2D visualization
# --------------------------------------------------

# from mpl_toolkits.mplot3d import Axes3D  # just to register 3D projection
# from sklearn.decomposition import PCA

# # 3D PCA
# pca = PCA(n_components=3)
# X_pca = pca.fit_transform(X_scaled)

# df_model["PC1"] = X_pca[:, 0]
# df_model["PC2"] = X_pca[:, 1]
# df_model["PC3"] = X_pca[:, 2]

# print("\nExplained variance by PC1, PC2, PC3:", pca.explained_variance_ratio_)
# print("Total variance in first 3 PCs:", pca.explained_variance_ratio_.sum())

# # 3D scatter plot
# fig = plt.figure(figsize=(9, 7))
# ax = fig.add_subplot(111, projection="3d")

# for c in range(k_final):
#     subset = df_model[df_model["cluster"] == c]
#     ax.scatter(
#         subset["PC1"],
#         subset["PC2"],
#         subset["PC3"],
#         s=np.clip(subset["TOI_per_game"] * 2.0, 10, 80),
#         alpha=0.6,
#         label=f"Cluster {c}",
#     )

# ax.set_xlabel("PC1")
# ax.set_ylabel("PC2")
# ax.set_zlabel("PC3")
# ax.set_title("Skater player-season clusters (3D PCA)")
# ax.legend()
# plt.tight_layout()
# plt.show()


# --------------------------------------------------
# 13. Save results
# --------------------------------------------------

df_model.to_csv("skater_merged_player_seasons_with_clusters.csv", index=False)
cluster_centers.to_csv("skater_merged_cluster_centers.csv")

print("\nSaved:")
print("  skater_merged_player_seasons_with_clusters.csv")
print("  skater_merged_cluster_centers.csv")
