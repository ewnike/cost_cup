import pandas as pd
import numpy as np

# ------------- config: set the team you want to study ---------
TEAM_ID = 16  # <-- change to the team_id you want to analyze
# --------------------------------------------------------------


# 1) Load line-level data
df_lines = pd.read_csv("line_level_with_corsi_and_archetypes.csv")

print("Columns in line-level file:", df_lines.columns.tolist())
print("Total line rows:", len(df_lines))

# sanity: only keep valid 3-player lines with positive TOI (should already be true)
df_lines = df_lines[(df_lines["n_players"] == 3) & (df_lines["toi_ev_min"] > 0)].copy()
print("Line rows after sanity filter:", len(df_lines))

# 2) Filter to the team of interest
df_team_lines = df_lines[df_lines["team_id"] == TEAM_ID].copy()
print(f"Lines for team {TEAM_ID}:", len(df_team_lines))

# 2a) Summary by archetype mix for this team
team_line_mix = (
    df_team_lines.groupby(["season", "c0", "c1", "c2"])
    .agg(
        n_lines=("game_id", "count"),
        total_toi_ev_min=("toi_ev_min", "sum"),
        p_win_corsi=("won_corsi", "mean"),
        mean_CF_pct=("CF_pct", "mean"),
        mean_CF60=("CF60", "mean"),
        mean_CA60=("CA60", "mean"),
    )
    .reset_index()
)

# Add share-of-time within each season
team_line_mix["toi_share_in_season"] = team_line_mix.groupby("season")[
    "total_toi_ev_min"
].transform(lambda x: x / x.sum())

print("\n=== Team line usage & results by season and archetype mix ===")
print(
    team_line_mix.sort_values(["season", "toi_share_in_season"], ascending=[True, False])
    .round(3)
    .to_string(index=False)
)

# 3) Team totals per game
team_game_totals = (
    df_lines.groupby(["game_id", "team_id", "season"])
    .agg(
        team_CF=("CF", "sum"),
        team_CA=("CA", "sum"),
        team_toi_ev_min=("toi_ev_min", "sum"),
    )
    .reset_index()
)

team_game_totals["team_CF_pct"] = team_game_totals["team_CF"] / (
    team_game_totals["team_CF"] + team_game_totals["team_CA"]
)

# Split into this team vs opponents
df_team_games = team_game_totals[team_game_totals["team_id"] == TEAM_ID].copy()
df_opp_games = team_game_totals[team_game_totals["team_id"] != TEAM_ID].copy()

# For each game, there should be exactly one row for TEAM_ID and one for the opponent.
df_opp_games = (
    df_opp_games.groupby("game_id")
    .agg(
        opp_CF=("team_CF", "sum"),
        opp_CA=("team_CA", "sum"),
        opp_toi_ev_min=("team_toi_ev_min", "sum"),
    )
    .reset_index()
)

df_opp_games["opp_CF_pct"] = df_opp_games["opp_CF"] / (
    df_opp_games["opp_CF"] + df_opp_games["opp_CA"]
)

# Join team and opponent totals per game
df_matchups = df_team_games.merge(df_opp_games, on="game_id", how="inner")

print("\n=== Per-game matchups for team", TEAM_ID, "===")
print(df_matchups[["season", "game_id", "team_CF_pct", "opp_CF_pct"]].head())
