"""
strength_utils.py.

Shared helpers for "strength state" filtering (legacy + modern).

Definition used here:
- Count SKATERS only (goalies excluded).
- Allow equal skater counts (5v5, 4v4, 3v3 OT, etc.)
- Exclude imbalanced skater counts (6v5, 5v4, 4v3, etc.)

Core exclude rule:
    exclude = (team_1 != team_2) & (team_1 <= 6) & (team_2 <= 6)

Notes:
- Legacy shifts do NOT have team_id; we merge from raw.game_skater_stats (skaters-only)
  and drop null team_id (filters goalies/unmatched).
- Modern shifts often already have team_id; goalie filtering depends on available columns.

"""

from __future__ import annotations

import pandas as pd


def get_num_players(shift_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute number of players (skaters) on ice at each time breakpoint.

    Returns columns: value (time), num_players
    """
    if shift_df.empty:
        return pd.DataFrame(columns=["value", "num_players"])

    melted = (
        pd.melt(
            shift_df,
            id_vars=["game_id", "player_id"],
            value_vars=["shift_start", "shift_end"],
        )
        .sort_values("value", ignore_index=True)
        .copy()
    )
    # shift_start => +1, shift_end => -1
    melted["change"] = 2 * (melted["variable"] == "shift_start").astype(int) - 1
    melted["num_players"] = melted["change"].cumsum()

    out = melted.groupby("value")["num_players"].last().reset_index()
    out = out[out["num_players"].shift() != out["num_players"]].reset_index(drop=True)
    return out


def ensure_team_id_on_shifts_legacy(
    game_shifts: pd.DataFrame,
    game_skater_stats: pd.DataFrame,
) -> pd.DataFrame:
    """
    Legacy: attach team_id to shifts by merging from game_skater_stats (skaters-only).

    Filters goalies/unmatched by dropping null team_id.
    """
    if game_shifts.empty:
        return game_shifts
    if game_skater_stats is None or game_skater_stats.empty:
        raise ValueError("game_skater_stats required for legacy team_id merge.")

    merged = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left",
    )

    # goalies/unmatched -> null team_id
    merged = merged.dropna(subset=["team_id"]).copy()
    merged["team_id"] = merged["team_id"].astype(int)
    return merged


def filter_goalies_modern(game_shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Modern goalie filter (only if you have a position/playerType column).

    If you don't have goalie info, return unchanged and handle elsewhere.
    """
    if game_shifts.empty:
        return game_shifts

    if "position" in game_shifts.columns:
        return game_shifts[game_shifts["position"] != "G"].copy()

    if "playerType" in game_shifts.columns:
        return game_shifts[~game_shifts["playerType"].isin(["G", "Goalie"])].copy()

    # no goalie marker available
    return game_shifts


def build_exclude_timeline_equal_strength(game_shifts_skaters: pd.DataFrame) -> pd.DataFrame:
    """
    Build an exclude timeline from SKATER shifts (team_id present, goalies removed).

    Allowed time:
      - Equal skater counts (5v5, 4v4, 3v3 OT, etc.)

    Excluded time:
      - Imbalanced skater counts up to 6 (6v5, 5v4, 4v3, etc.)
        exclude = (team_1 != team_2) & (team_1 <= 6) & (team_2 <= 6)

    Returns columns:
      time (breakpoints), team_1, team_2, exclude
    """
    if game_shifts_skaters.empty:
        return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

    # Ensure numeric timepoints
    if "shift_start" in game_shifts_skaters.columns:
        game_shifts_skaters = game_shifts_skaters.copy()
        game_shifts_skaters["shift_start"] = game_shifts_skaters["shift_start"].astype(int)
        game_shifts_skaters["shift_end"] = game_shifts_skaters["shift_end"].astype(int)

    team_ids = sorted(game_shifts_skaters["team_id"].dropna().unique())
    if len(team_ids) != 2:
        return pd.DataFrame(columns=["time", "team_1", "team_2", "exclude"])

    t1, t2 = team_ids
    s1 = game_shifts_skaters[game_shifts_skaters["team_id"] == t1]
    s2 = game_shifts_skaters[game_shifts_skaters["team_id"] == t2]

    df1 = get_num_players(s1).rename(columns={"value": "time", "num_players": "team_1"})
    df2 = get_num_players(s2).rename(columns={"value": "time", "num_players": "team_2"})

    df_ex = (
        pd.concat([df1, df2], ignore_index=True)
        .sort_values("time", ignore_index=True)
        .ffill()
        .bfill()
    )
    df_ex = df_ex[df_ex["time"].shift(-1) != df_ex["time"]].reset_index(drop=True)

    df_ex["exclude"] = (
        (df_ex["team_1"] != df_ex["team_2"]) & (df_ex["team_1"] <= 6) & (df_ex["team_2"] <= 6)
    )

    return df_ex


def apply_exclude_to_plays(plays: pd.DataFrame, exclude_timeline: pd.DataFrame) -> pd.DataFrame:
    """
    Filter plays using exclude timeline (expects plays has 'time' as int seconds).

    Keeps plays where exclude == False.
    """
    if plays.empty or exclude_timeline.empty:
        return plays

    times = plays["time"].to_numpy()
    ex_t = exclude_timeline["time"].to_numpy()
    ex_flag = exclude_timeline["exclude"].to_numpy(bool)

    idx = ex_t.searchsorted(times, side="right") - 1
    idx[idx < 0] = 0
    idx = idx.clip(0, len(ex_flag) - 1)

    mask = ex_flag[idx]
    return plays.loc[~mask].copy()
