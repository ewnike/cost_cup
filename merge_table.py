"""Merge_table."""

import pandas as pd

df_corsi = pd.read_csv("player_season_corsi_2015_2018.csv")
df_season = pd.read_csv("skater_player_seasons_with_clusters.csv")
# you now have: player_id, team_id, season, corsi_for, corsi_against,
# corsi, cf_percent, time_on_ice, game_count, cap_hit

df_corsi["toi_corsi_min"] = df_corsi["time_on_ice"] / 60.0
df_corsi = df_corsi[df_corsi["toi_corsi_min"] > 0].copy()

df_corsi["CF60"] = df_corsi["corsi_for"] / df_corsi["toi_corsi_min"] * 60.0
df_corsi["CA60"] = df_corsi["corsi_against"] / df_corsi["toi_corsi_min"] * 60.0
df_corsi["CF_pct"] = df_corsi["cf_percent"]  # already 0–1 if you set it that way

# merge into your existing df_season:
df_merged = df_season.merge(
    df_corsi[["player_id", "season", "CF60", "CA60", "CF_pct", "cap_hit"]],
    on=["player_id", "season"],
    how="left",
)
