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

import os
import pathlib

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

# from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from db_utils import get_db_engine

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

TARGET_SEASONS = ["20152016", "20162017", "20172018"]
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
                    g.season::text AS season,
                    gss.game_id,
                    gss.player_id,
                    gss.team_id,
                    gss."timeOnIce"            AS time_on_ice,
                    gss.assists,
                    gss.goals,
                    gss.shots,
                    gss.hits,
                    gss."powerPlayGoals"       AS power_play_goals,
                    gss."powerPlayAssists"     AS power_play_assists,
                    gss."penaltyMinutes"       AS penalty_minutes,
                    gss."faceOffWins"          AS face_off_wins,
                    gss."faceoffTaken"         AS faceoff_taken,
                    gss.takeaways,
                    gss.giveaways,
                    gss."shortHandedGoals"     AS short_handed_goals,
                    gss."shortHandedAssists"   AS short_handed_assists,
                    gss.blocked,
                    gss."plusMinus"            AS plus_minus,
                    gss."evenTimeOnIce"        AS even_time_on_ice,
                    gss."shortHandedTimeOnIce" AS short_handed_time_on_ice,
                    gss."powerPlayTimeOnIce"   AS power_play_time_on_ice
                FROM raw.game_skater_stats gss
                JOIN raw.game g
                ON g.game_id = gss.game_id
                WHERE g.season >= 20152016;
            """),
            conn,
        )
finally:
    engine.dispose()

df_games["season"] = df_games["season"].astype(str)
df_games = df_games[df_games["season"].isin(TARGET_SEASONS)].copy()
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
    "game_id": "nunique",
    "time_on_ice": "sum",
    "even_time_on_ice": "sum",
    "power_play_time_on_ice": "sum",
    "short_handed_time_on_ice": "sum",
    "goals": "sum",
    "assists": "sum",
    "shots": "sum",
    "hits": "sum",
    "power_play_goals": "sum",
    "power_play_assists": "sum",
    "short_handed_goals": "sum",
    "short_handed_assists": "sum",
    "penalty_minutes": "sum",
    "face_off_wins": "sum",
    "faceoff_taken": "sum",
    "takeaways": "sum",
    "giveaways": "sum",
    "blocked": "sum",
    "plus_minus": "sum",  # ✅ THIS fixes the typo situation
}

df_season = df_games.groupby(group_cols).agg(agg_dict).reset_index()
df_season = df_season.rename(columns={"game_id": "games_played"})

print("Player-season rows:", len(df_season))


# --------------------------------------------------
# 4. Filter low-TOI players
# --------------------------------------------------
# timeOnIce is seconds -> convert to minutes

df_season["toi_total_min"] = df_season["time_on_ice"] / 60.0
df_season["toi_ev_min"] = df_season["even_time_on_ice"] / 60.0
df_season["toi_pp_min"] = df_season["power_play_time_on_ice"] / 60.0
df_season["toi_sh_min"] = df_season["short_handed_time_on_ice"] / 60.0

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
df_season["PIM60"] = df_season["penalty_minutes"] / df_season["toi_total_min"] * 60.0

# Special teams
df_season["PP_PTS60"] = (
    (df_season["power_play_goals"] + df_season["power_play_assists"])
    / df_season["toi_total_min"]
    * 60.0
)
df_season["SH_PTS60"] = (
    (df_season["short_handed_goals"] + df_season["short_handed_assists"])
    / df_season["toi_total_min"]
    * 60.0
)

# Usage by situation (minutes per game)
df_season["EV_TOI_per_game"] = df_season["toi_ev_min"] / df_season["games_played"]
df_season["PP_TOI_per_game"] = df_season["toi_pp_min"] / df_season["games_played"]
df_season["SH_TOI_per_game"] = df_season["toi_sh_min"] / df_season["games_played"]
df_season["TOI_per_game"] = df_season["toi_total_min"] / df_season["games_played"]

# Faceoff win %
df_season["fo_attempts"] = df_season["faceoff_taken"]
df_season["fo_win_pct"] = np.where(
    df_season["fo_attempts"] > 0,
    df_season["face_off_wins"] / df_season["fo_attempts"],
    np.nan,
)

df_season["fo_win_pct"] = df_season["fo_win_pct"].fillna(df_season["fo_win_pct"].mean())

# plusMinus per 60 just as another flavor
df_season["PLUSMINUS60"] = df_season["plus_minus"] / df_season["toi_total_min"] * 60.0


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

# Keep only legacy seasons (and make sure season is comparable)
TARGET_SEASONS = ["20152016", "20162017", "20172018"]
df_merged["season"] = df_merged["season"].astype(str)

# "before" = after TOI filter + Corsi merge, but before dropna(features)
before = df_merged[df_merged["season"].isin(TARGET_SEASONS)].copy()

# Final modeling set = drop rows with any NaN in the features
df_model = before.dropna(subset=features).copy()

print("Dropped by season (due to dropna(features)):")
print(
    before.groupby("season").size().sub(df_model.groupby("season").size(), fill_value=0).astype(int)
)


X = df_model[features].values


# --------------------------------------------------
# 7. Scale features
# --------------------------------------------------

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


# --------------------------------------------------
# 8. Choose k (Option 1: force k=3)
# --------------------------------------------------
k_final = 3

# --------------------------------------------------
# 9. Fit final model & assign clusters
# --------------------------------------------------
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
print(cluster_centers.round(2).to_string())

# --------------------------------------------------
# 12. Save results (no duplicate confusing filenames)
# --------------------------------------------------
out_players = f"skater_merged_player_seasons_with_clusters_k{k_final}.csv"
out_centers = f"skater_merged_cluster_centers_k{k_final}.csv"

df_model.to_csv(out_players, index=False)
cluster_centers.to_csv(out_centers)

print("\nSaved:")
print(f"  {out_players}")
print(f"  {out_centers}")
