"""Merge_table."""

import os
import pathlib

import pandas as pd

from stats_utils import add_corsi_rates_and_merge

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

df_corsi = pd.read_csv("player_season_corsi_2015_2018.csv")
df_season = pd.read_csv("skater_player_seasons_with_clusters.csv")

df_merged = add_corsi_rates_and_merge(df_season, df_corsi)
df_merged.to_csv("skater_player_seasons_with_clusters_and_rates.csv", index=False)
print("Saved skater_player_seasons_with_clusters_and_rates.csv")
