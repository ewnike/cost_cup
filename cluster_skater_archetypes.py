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

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

# from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from db_utils import get_db_engine

engine = get_db_engine()

# --------------------------------------------------
# 1. Load game-by-game skater stats
# --------------------------------------------------

# csv_path = "game_skater_stats.csv"  # <-- change if needed
# df_games = pd.read_csv(csv_path)


try:
    with engine.connect() as conn:
        df_games = pd.read_sql_query(
            text("""
                SELECT
                    g.season,
                    gss.game_id,
                    gss.player_id,
                    gss.team_id,
                    gss.timeOnIce,
                    gss.assists,
                    gss.goals,
                    gss.shots,
                    gss.hits,
                    gss.powerPlayGoals,
                    gss.powerPlayAssists,
                    gss.penaltyMinutes,
                    gss.faceOffWins,
                    gss.faceoffTaken,
                    gss.takeaways,
                    gss.giveaways,
                    gss.shortHandedGoals,
                    gss.shortHandedAssists,
                    gss.blocked,
                    gss.plusMinus,
                    gss.evenTimeOnIce,
                    gss.shortHandedTimeOnIce,
                    gss.powerPlayTimeOnIce
                FROM game_skater_stats gss
                JOIN game g
                    ON g.game_id = gss.game_id
                WHERE g.season >= 20152016
            """),
            conn,
        )
finally:
    engine.dispose()

print("df_games seasons:", sorted(df_games["season"].astype(str).unique())[-10:])
print("player-game rows:", len(df_games))


# --------------------------------------------------
# 2. Derive season from game_id
# --------------------------------------------------
# game_id like 2016020045 -> season "20162017"


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


# --- CORSI MERGE BLOCK ---
df_corsi = pd.read_csv("player_season_corsi_all.csv")
print("df_season season dtype:", df_season["season"].dtype, "sample:", df_season["season"].iloc[0])
print("df_corsi  season dtype:", df_corsi["season"].dtype, "sample:", df_corsi["season"].iloc[0])
# make sure season types match
df_corsi["season"] = df_corsi["season"].astype(str)
df_season["season"] = df_season["season"].astype(str)
# keep only seasons that exist in raw_corsi export
valid_seasons = set(df_corsi["season"].unique())
df_season = df_season[df_season["season"].isin(valid_seasons)].copy()
print("df_season seasons after filter:", sorted(df_season["season"].unique()))
# merge corsi onto df_season
df_merged = df_season.merge(
    df_corsi[["player_id", "season", "corsi_for", "corsi_against", "cf_percent"]],
    on=["player_id", "season"],
    how="left",
)

# compute CF60/CA60 using TOI from df_season (pick the denominator you want)
df_merged["toi_corsi_min"] = df_merged["toi_ev_min"]  # or "toi_total_min"

df_merged["CF60"] = (df_merged["corsi_for"] / df_merged["toi_corsi_min"]) * 60.0
df_merged["CA60"] = (df_merged["corsi_against"] / df_merged["toi_corsi_min"]) * 60.0
df_merged["CF_pct"] = df_merged["cf_percent"]

# clean up inf/NaN
df_merged[["CF60", "CA60"]] = df_merged[["CF60", "CA60"]].replace(
    [float("inf"), -float("inf")], pd.NA
)
df_merged["CF_pct"] = df_merged["CF_pct"].fillna(0.0)
print("Corsi match rate:", df_merged["corsi_for"].notna().mean())


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
# 12. Save results
# --------------------------------------------------

# df_model.to_csv("skater_merged_player_seasons_with_clusters.csv", index=False)
# cluster_centers.to_csv("skater_merged_cluster_centers.csv")
df_model.to_csv("skater_merged_player_seasons_with_clusters_all.csv", index=False)
cluster_centers.to_csv("skater_merged_cluster_centers_all.csv", index=False)

print("\nSaved:")
print("  skater_merged_player_seasons_with_clusters.csv")
print("  skater_merged_cluster_centers.csv")
