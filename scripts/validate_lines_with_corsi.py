"""
validate_lines_with_corsi.

Goal:
- Use game-by-game Corsi + your player archetype clusters
- Approximate lines (3 skaters per line per team per game)
- Compute line-level CF, CA, CF%, CF60, CA60
- Summarize performance by archetype mix (c0, c1, c2)
"""

import numpy as np
import pandas as pd


def game_id_to_season(gid: int) -> str:
    """Derive NHL season out of game_id."""
    year_start = gid // 1_000_000
    return f"{year_start}{year_start + 1}"


# -----------------------------
# 1. Load data
# -----------------------------

print("Loading game_skater_stats.csv...")
df_games = pd.read_csv("game_skater_stats.csv")

print("Loading player_season_corsi_2015_2018.csv...")
df_corsi = pd.read_csv("player_season_corsi_2015_2018.csv")

print("Loading skater_merged_player_seasons_with_clusters.csv...")
df_clusters = pd.read_csv("skater_merged_player_seasons_with_clusters.csv")
df_clusters["season"] = df_clusters["season"].astype(str)


# -----------------------------
# 2. Add season to game + corsi
# -----------------------------

df_games["season"] = df_games["game_id"].astype(int).apply(game_id_to_season).astype(str)
df_corsi["season"] = df_corsi["game_id"].astype(int).apply(game_id_to_season).astype(str)

# keep only seasons we clustered (defensive but safe)
target_seasons = ["20152016", "20162017", "20172018"]
df_games = df_games[df_games["season"].isin(target_seasons)].copy()
df_corsi = df_corsi[df_corsi["season"].isin(target_seasons)].copy()

print("Game rows after season filter:", len(df_games))
print("Corsi rows after season filter:", len(df_corsi))


# -----------------------------
# 3. Merge boxscore + Corsi at player-game level
# -----------------------------

df_pg = df_games.merge(
    df_corsi,
    on=["game_id", "player_id", "team_id", "season"],
    how="inner",
)

print("Player-game rows after merge:", len(df_pg))


# -----------------------------
# 4. Attach player-season cluster (archetype)
# -----------------------------

df_pg = df_pg.merge(
    df_clusters[["player_id", "season", "cluster"]],
    on=["player_id", "season"],
    how="left",
)

print("Rows with non-null cluster:", df_pg["cluster"].notna().sum())


# -----------------------------
# 5. Approximate lines per game/team
# -----------------------------
# We'll:
# - group by (game_id, team_id)
# - sort by evenTimeOnIce
# - take top 12 skaters
# - assign them as lines of 3 (L1-L4)
# This is a rough but useful approximation.


def assign_lines(group, line_size=3, max_lines=4):
    """Assign players to lines."""
    g = group.sort_values("evenTimeOnIce", ascending=False).reset_index(drop=True)
    g = g.iloc[: line_size * max_lines]  # top N skaters
    if g.empty:
        return g
    g["line_no"] = (g.index // line_size) + 1  # 1..4
    return g


df_lines_players = (
    df_pg.groupby(["game_id", "team_id"], group_keys=False)
    .apply(assign_lines)
    .reset_index(drop=True)
)

print("Player-game rows assigned to lines:", len(df_lines_players))


# -----------------------------
# 6. Aggregate to line-level Corsi + archetype mix
# -----------------------------

line_agg = (
    df_lines_players.groupby(["game_id", "team_id", "season", "line_no"])
    .agg(
        n_players=("player_id", "nunique"),
        # counts of each cluster archetype
        c0=("cluster", lambda x: (x == 0).sum()),
        c1=("cluster", lambda x: (x == 1).sum()),
        c2=("cluster", lambda x: (x == 2).sum()),
        # TOI and Corsi totals
        toi_ev=("evenTimeOnIce", "sum"),  # seconds at EV
        CF=("corsi_for", "sum"),
        CA=("corsi_against", "sum"),
    )
    .reset_index()
)

# keep only proper 3-man lines
line_agg = line_agg[line_agg["n_players"] == 3].copy()

# convert TOI to minutes
line_agg["toi_ev_min"] = line_agg["toi_ev"] / 60.0
line_agg = line_agg[line_agg["toi_ev_min"] > 0].copy()

# compute CF%, CF60, CA60
line_agg["CF_pct"] = line_agg["CF"] / (line_agg["CF"] + line_agg["CA"])
line_agg["CF60"] = line_agg["CF"] / line_agg["toi_ev_min"] * 60.0
line_agg["CA60"] = line_agg["CA"] / line_agg["toi_ev_min"] * 60.0

print("Line-level rows:", len(line_agg))


# -----------------------------
# 7. Summarize by archetype mix (c0, c1, c2)
# -----------------------------

comp_cols = ["c0", "c1", "c2"]

line_summary = (
    line_agg.groupby(comp_cols)
    .agg(
        n_lines=("game_id", "count"),
        mean_CF_pct=("CF_pct", "mean"),
        mean_CF60=("CF60", "mean"),
        mean_CA60=("CA60", "mean"),
        mean_toi_ev_min=("toi_ev_min", "mean"),
    )
    .reset_index()
    .sort_values("mean_CF_pct", ascending=False)
)

print("\n=== Line Corsi performance by archetype composition ===")
print(line_summary.round(3).to_string(index=False))


# -----------------------------
# 8. Save outputs for further analysis / plotting
# -----------------------------

line_agg.to_csv("line_level_with_corsi_and_archetypes.csv", index=False)
line_summary.to_csv("line_archetype_corsi_summary.csv", index=False)

print("\nSaved:")
print("  line_level_with_corsi_and_archetypes.csv")
print("  line_archetype_corsi_summary.csv")
