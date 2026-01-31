"""Merge_table."""

import pandas as pd

from stats_utils import add_corsi_rates_and_merge

df_corsi = pd.read_csv("player_season_corsi_2015_2018.csv")
df_season = pd.read_csv("skater_player_seasons_with_clusters.csv")

df_merged = add_corsi_rates_and_merge(df_season, df_corsi)
df_merged.to_csv("skater_player_seasons_with_clusters_and_rates.csv", index=False)
print("Saved skater_player_seasons_with_clusters_and_rates.csv")
