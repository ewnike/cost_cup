"""
compute base rates for player archetype.

Calculate empirical priors needed for Bayesian
applications.

Author: Eric Winiecke
Date: December 6, 2025.
"""

import os
import pathlib

import pandas as pd

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")


df_corsi = pd.read_csv("player_game_corsi_2015_2018.csv")
df_clusters = pd.read_csv("skater_merged_player_seasons_with_clusters.csv")

# make season type consistent
df_clusters["season"] = df_clusters["season"].astype(str)


def game_id_to_season(gid: int) -> str:
    """Attach season to game-level Corsi via game_id -> season."""
    year_start = gid // 1_000_000
    return f"{year_start}{year_start + 1}"


df_corsi["season"] = df_corsi["game_id"].astype(int).apply(game_id_to_season).astype(str)

# merge in cluster
df_pg = df_corsi.merge(
    df_clusters[["player_id", "season", "cluster"]],
    on=["player_id", "season"],
    how="inner",
)

# define some binary outcomes per game
df_pg["won_corsi"] = df_pg["corsi"] > 0
df_pg["good_cf_pct"] = df_pg["cf_percent"] >= 50
df_pg["big_positive_game"] = df_pg["corsi"] >= 5

cluster_base = df_pg.groupby("cluster").agg(
    games=("game_id", "count"),
    mean_cf=("cf_percent", "mean"),
    p_win_corsi=("won_corsi", "mean"),
    p_cf50_plus=("good_cf_pct", "mean"),
    p_big_plus=("big_positive_game", "mean"),
)
print(cluster_base)

print(df_pg["corsi"].describe())
print(df_pg["corsi"].quantile([0.5, 0.7, 0.8, 0.9, 0.95]))
