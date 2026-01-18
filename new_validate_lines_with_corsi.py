"""
validate_lines_with_corsi.py.

Line-level analysis using archetypes.

Inputs:
- game_skater_stats.csv
- player_game_corsi_2015_2018.csv
- skater_merged_player_seasons_with_clusters.csv

Outputs:
- line_level_with_corsi_and_archetypes.csv
- line_archetype_corsi_summary.csv
"""

import pandas as pd


def game_id_to_season(gid: int) -> str:
    """Map NHL game_id like 2016020045 -> '20162017'."""
    year_start = gid // 1_000_000
    return f"{year_start}{year_start + 1}"


# -----------------------------
# 1. Load data
# -----------------------------

print("Loading game_skater_stats.csv...")
df_games = pd.read_csv("game_skater_stats.csv")

print("Loading player_game_corsi_2015_2018.csv...")
df_corsi = pd.read_csv("player_game_corsi_2015_2018.csv")

print("Loading skater_merged_player_seasons_with_clusters.csv...")
df_clusters = pd.read_csv("skater_merged_player_seasons_with_clusters.csv")
df_clusters["season"] = df_clusters["season"].astype(str)


# -----------------------------
# 2. Attach season
# -----------------------------

df_games["season"] = df_games["game_id"].astype(int).apply(game_id_to_season).astype(str)
df_corsi["season"] = df_corsi["game_id"].astype(int).apply(game_id_to_season).astype(str)

target_seasons = ["20152016", "20162017", "20172018"]
df_games = df_games[df_games["season"].isin(target_seasons)].copy()
df_corsi = df_corsi[df_corsi["season"].isin(target_seasons)].copy()

print("Game rows after season filter:", len(df_games))
print("Corsi rows after season filter:", len(df_corsi))


# -----------------------------
# 3. Merge game stats + Corsi at player-game level
# -----------------------------

merge_keys = ["game_id", "player_id", "team_id", "season"]

df_pg = df_games.merge(
    df_corsi,
    on=merge_keys,
    how="inner",
)

print("Player-game rows after merge:", len(df_pg))


# -----------------------------
# 4. Attach archetype cluster (player-season)
# -----------------------------

df_pg = df_pg.merge(
    df_clusters[["player_id", "season", "cluster"]],
    on=["player_id", "season"],
    how="left",
)

print("Player-game rows with cluster:", df_pg["cluster"].notna().sum())

# Drop rows with missing cluster (e.g. low-TOI seasons that didn't meet 200 min)
df_pg = df_pg[df_pg["cluster"].notna()].copy()
df_pg["cluster"] = df_pg["cluster"].astype(int)
print("Player-game rows after dropping missing clusters:", len(df_pg))


# -----------------------------
# 5. Approximate lines by EV TOI
# -----------------------------
# Group by game+team, sort by evenTimeOnIce descending,
# keep top 12 skaters, assign them to 4 lines of 3.

if "evenTimeOnIce" not in df_pg.columns:
    raise ValueError("evenTimeOnIce column not found in game_skater_stats.csv")


def assign_lines(group: pd.DataFrame, line_size: int = 3, max_lines: int = 4) -> pd.DataFrame:
    g = group.sort_values("evenTimeOnIce", ascending=False).reset_index(drop=True)
    g = g.iloc[: line_size * max_lines]  # top 12 skaters by EV TOI
    if g.empty:
        return g
    g["line_no"] = (g.index // line_size) + 1  # 1..4
    return g


df_lines_players = (
    df_pg.groupby(["game_id", "team_id"], group_keys=False)
    .apply(assign_lines, include_groups=False)
    .reset_index(drop=True)
)

print("Player-game rows assigned to lines:", len(df_lines_players))


# -----------------------------
# 6. Aggregate to line-level Corsi + archetype mix
# -----------------------------

# If timeOnIce is in seconds, convert EV TOI to minutes
df_lines_players["toi_ev_min"] = df_lines_players["evenTimeOnIce"] / 60.0

line_agg = (
    df_lines_players.groupby(["game_id", "team_id", "season", "line_no"])
    .agg(
        n_players=("player_id", "nunique"),
        c0=("cluster", lambda x: (x == 0).sum()),
        c1=("cluster", lambda x: (x == 1).sum()),
        c2=("cluster", lambda x: (x == 2).sum()),
        toi_ev_min=("toi_ev_min", "sum"),
        CF=("corsi_for", "sum"),
        CA=("corsi_against", "sum"),
    )
    .reset_index()
)

# keep only true 3-skater lines
line_agg = line_agg[line_agg["n_players"] == 3].copy()

# require some TOI
line_agg = line_agg[line_agg["toi_ev_min"] > 0].copy()

# compute Corsi metrics
line_agg["CF_pct"] = line_agg["CF"] / (line_agg["CF"] + line_agg["CA"])
line_agg["CF60"] = line_agg["CF"] / line_agg["toi_ev_min"] * 60.0
line_agg["CA60"] = line_agg["CA"] / line_agg["toi_ev_min"] * 60.0

line_agg["won_corsi"] = line_agg["CF"] > line_agg["CA"]
line_agg["good_cf_pct"] = line_agg["CF_pct"] >= 0.5

print("Line-level rows:", len(line_agg))


# -----------------------------
# 7. Summarize by archetype composition (c0, c1, c2)
# -----------------------------

comp_cols = ["c0", "c1", "c2"]

line_summary = (
    line_agg.groupby(comp_cols)
    .agg(
        n_lines=("game_id", "count"),
        p_win_corsi=("won_corsi", "mean"),
        mean_CF_pct=("CF_pct", "mean"),
        mean_CF60=("CF60", "mean"),
        mean_CA60=("CA60", "mean"),
        mean_toi_ev_min=("toi_ev_min", "mean"),
    )
    .reset_index()
)

# only keep compositions that actually sum to 3 skaters
line_summary = line_summary[line_summary["c0"] + line_summary["c1"] + line_summary["c2"] == 3]

line_summary = line_summary.sort_values("p_win_corsi", ascending=False)

print(
    "\n=== Line Corsi performance by archetype composition (Drivers=c0, Anchors=c1, Crashers=c2) ==="
)
print(line_summary.round(3).to_string(index=False))


# -----------------------------
# 8. Save outputs
# -----------------------------

line_agg.to_csv("line_level_with_corsi_and_archetypes.csv", index=False)
line_summary.to_csv("line_archetype_corsi_summary.csv", index=False)

print("\nSaved:")
print("  line_level_with_corsi_and_archetypes.csv")
print("  line_archetype_corsi_summary.csv")
