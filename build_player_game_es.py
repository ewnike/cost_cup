"""
Build mart.player_game_es_{season} for modern seasons (20182019+).

Definition (skater-strength based):
- Skaters only (goalies excluded via shifts position == 'G' when available).
- Keep equal-strength segments where both teams have the same number of skaters
  (e.g., 5v5, 4v4, 3v3 OT).
- Exclude imbalanced skater segments (e.g., 6v5 empty net / delayed penalty,
  5v4 power plays, 4v3 OT power plays).
  Rule used: exclude = (team_1 != team_2) AND team_1 <= 6 AND team_2 <= 6

Metrics:
- CF/CA: counted from play-by-play shot attempts (Shot, Missed Shot, Blocked Shot, Goal)
  that occur during included (non-excluded) time.
- TOI (toi_sec): computed from player shifts with excluded-interval overlap removed.
- Rates: cf60, ca60, and cf_percent derived from CF/CA and toi_sec.

Inputs:
- derived.game_plays_{season}_from_raw_pbp
- raw.raw_shifts_resolved
- dim.dim_team_code

Output:
- mart.player_game_es_{season} with:
  (game_id, player_id, team_id, cf, ca, toi_sec, cf60, ca60, cf_percent)

Author: Eric Winiecke (standardized build script)

"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from constants import SCHEMA, SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger
from schema_utils import fq
from strength_utils import (
    apply_exclude_to_plays,
    build_exclude_timeline_equal_strength,
    filter_goalies_modern,
)

logger = setup_logger()


def exclude_intervals(df_exclude: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Convert df_exclude rows into [start,end) intervals where exclude=True."""
    if df_exclude.empty:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64)

    t = df_exclude["time"].to_numpy(dtype=np.int64)
    # end boundary is next time; last row end doesn't matter much
    t_next = np.r_[t[1:], t[-1]]
    mask = df_exclude["exclude"].to_numpy(bool)

    starts = t[mask]
    ends = t_next[mask]
    good = ends > starts
    return starts[good], ends[good]


def overlap_seconds(
    interval_start: np.ndarray, interval_end: np.ndarray, ex_start: np.ndarray, ex_end: np.ndarray
) -> np.ndarray:
    """Overlap between many [interval_start, interval_end) and exclude intervals."""
    out = np.zeros(len(interval_start), dtype=np.int64)
    if len(ex_start) == 0:
        return out

    for i in range(len(ex_start)):
        a = np.maximum(interval_start, ex_start[i])
        b = np.minimum(interval_end, ex_end[i])
        out += np.maximum(0, b - a)
    return out


def merge_intervals(starts: np.ndarray, ends: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Merge overlapping [start,end) intervals."""
    if len(starts) == 0:
        return starts, ends
    order = np.argsort(starts)
    s = starts[order]
    e = ends[order]

    out_s = [int(s[0])]
    out_e = [int(e[0])]

    for i in range(1, len(s)):
        if s[i] <= out_e[-1]:  # overlap/touch
            out_e[-1] = max(out_e[-1], int(e[i]))
        else:
            out_s.append(int(s[i]))
            out_e.append(int(e[i]))

    return np.array(out_s, dtype=np.int64), np.array(out_e, dtype=np.int64)


def build_es_toi_for_game(gs_game: pd.DataFrame) -> pd.DataFrame:
    """
    Docstring for build_es_toi_for_game.

    :param gs_game: Description
    :type gs_game: pd.DataFrame
    :return: Description
    :rtype: DataFrame
    """
    gs_game = filter_goalies_modern(gs_game)

    df_ex = build_exclude_timeline_equal_strength(gs_game)
    ex_s, ex_e = exclude_intervals(df_ex)

    gs_game = gs_game.copy()
    gs_game["shift_start"] = gs_game["shift_start"].astype(np.int64)
    gs_game["shift_end"] = gs_game["shift_end"].astype(np.int64)

    rows = []
    for (gid, pid, tid), grp in gs_game.groupby(["game_id", "player_id", "team_id"], sort=False):
        s = grp["shift_start"].to_numpy(np.int64)
        e = grp["shift_end"].to_numpy(np.int64)

        # merge overlaps first
        ms, me = merge_intervals(s, e)

        dur = np.maximum(0, me - ms)
        ov = overlap_seconds(ms, me, ex_s, ex_e)
        es = dur - ov
        es[es < 0] = 0

        rows.append((gid, pid, tid, int(es.sum())))

    return pd.DataFrame(rows, columns=["game_id", "player_id", "team_id", "toi_sec"])


def update_corsi_counts(
    df_corsi: pd.DataFrame, event: pd.Series, game_shifts: pd.DataFrame
) -> None:
    """Mutates df_corsi cf/ca in-place for one event."""
    time = int(event["time"])
    team_for = int(event["team_id_for"])
    team_against = int(event["team_id_against"])

    on_ice = game_shifts[(game_shifts["shift_start"] <= time) & (game_shifts["shift_end"] >= time)]
    players_for = on_ice.loc[on_ice["team_id"] == team_for, "player_id"].to_numpy()
    players_against = on_ice.loc[on_ice["team_id"] == team_against, "player_id"].to_numpy()

    ev = event["event"]
    if ev in ("Shot", "Goal", "Missed Shot"):
        df_corsi.loc[df_corsi["player_id"].isin(players_for), "cf"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against), "ca"] += 1
    elif ev == "Blocked Shot":
        df_corsi.loc[df_corsi["player_id"].isin(players_for), "ca"] += 1
        df_corsi.loc[df_corsi["player_id"].isin(players_against), "cf"] += 1


def build_player_game_es_for_season(season: int) -> pd.DataFrame:
    """Compute ES player-game stats for a season and return a dataframe."""
    engine = get_db_engine()

    plays_view = fq("derived", f"game_plays_{season}_from_raw_pbp")
    shifts_resolved = fq("raw", "raw_shifts_resolved")
    dim_team_code = fq("dim", "dim_team_code")

    gp = pd.read_sql(f"SELECT * FROM {plays_view}", engine)

    # ---- PATCH: normalize play time + event columns + team ids ----

    # time column
    if "time" not in gp.columns:
        if "game_seconds" in gp.columns:
            gp = gp.rename(columns={"game_seconds": "time"})
        else:
            raise KeyError(f"{plays_view} missing time/game_seconds")

    # event column
    if "event" not in gp.columns and "event_type" in gp.columns:
        gp = gp.rename(columns={"event_type": "event"})

    # normalize event labels to match your later logic
    event_map = {
        "SHOT": "Shot",
        "GOAL": "Goal",
        "MISS": "Missed Shot",
        "BLOCK": "Blocked Shot",
    }
    gp["event"] = gp["event"].map(event_map)

    # keep only corsi events early (prevents weird rows later)
    gp = gp.dropna(subset=["time", "event"]).copy()
    print("Corsi events present:", sorted(gp["event"].unique()))

    # map team codes -> team_id using dim_team_code
    team_map = pd.read_sql("SELECT team_code, team_id FROM dim.dim_team_code", engine)

    gp = gp.merge(
        team_map.rename(columns={"team_code": "home_team", "team_id": "home_team_id"}),
        on="home_team",
        how="left",
    )
    gp = gp.merge(
        team_map.rename(columns={"team_code": "away_team", "team_id": "away_team_id"}),
        on="away_team",
        how="left",
    )
    gp = gp.merge(
        team_map.rename(columns={"team_code": "event_team", "team_id": "event_team_id"}),
        on="event_team",
        how="left",
    )

    # derive for/against ids
    gp["team_id_for"] = gp["event_team_id"]
    gp["team_id_against"] = np.where(
        gp["event_team_id"] == gp["home_team_id"],
        gp["away_team_id"],
        gp["home_team_id"],
    )

    print(
        f"[{season}] missing team ids:",
        gp["team_id_for"].isna().sum(),
        gp["team_id_against"].isna().sum(),
    )
    # final cleanup: require both team ids
    gp = gp.dropna(subset=["team_id_for", "team_id_against"]).copy()
    gp["team_id_for"] = gp["team_id_for"].astype("int64")
    gp["team_id_against"] = gp["team_id_against"].astype("int64")
    gp["time"] = pd.to_numeric(gp["time"], errors="coerce").astype("int64")

    # --------------------------------------------------------------

    gs = pd.read_sql(
        f"""
        SELECT
        rs.game_id,
        rs.player_id_resolved AS player_id,
        dt.team_id,
        rs.position,
        rs.game_period AS period,
        rs.seconds_start AS shift_start,
        rs.seconds_end   AS shift_end
        FROM {shifts_resolved} rs
        JOIN {dim_team_code} dt
        ON dt.team_code = rs.team
        WHERE rs.season = {int(season)}
        AND rs.session = 'R'
        AND rs.position <> 'G'
        AND rs.seconds_end > rs.seconds_start
        """,
        engine,
    )

    engine.dispose()

    out_rows: list[pd.DataFrame] = []

    print(sorted(gp["event"].dropna().unique())[:50])

    n_goalie = (gs["position"] == "G").sum()
    if n_goalie:
        logger.warning("Found %s goalie shift rows in gs (should be 0).", int(n_goalie))

    for game_id, gp_game in gp.groupby("game_id", sort=False):
        gs_game = gs[gs["game_id"] == game_id]
        if gp_game.empty or gs_game.empty:
            continue

        # Ensure plays have numeric time and are corsi-relevant
        gp_game = gp_game.copy()
        gp_game["time"] = pd.to_numeric(gp_game["time"], errors="coerce").astype("Int64")
        gp_game = gp_game.dropna(subset=["time"])
        gp_game["time"] = gp_game["time"].astype("int64")
        gp_game = gp_game[
            gp_game["event"].isin(["Shot", "Goal", "Missed Shot", "Blocked Shot"])
        ].dropna(subset=["team_id_for", "team_id_against"])

        if gp_game.empty:
            continue

        # ✅ filter goalies once; use same gs_game for TOI + CF/CA
        gs_game = filter_goalies_modern(gs_game)
        gs_game["shift_start"] = gs_game["shift_start"].astype(int)
        gs_game["shift_end"] = gs_game["shift_end"].astype(int)

        # sanity: shifts should be within a single game's clock
        if gs_game["shift_end"].max() > 3900:
            logger.warning(
                "game_id=%s shift_end max looks high: %s",
                game_id,
                int(gs_game["shift_end"].max()),
            )
        # TOI ES
        toi_game = build_es_toi_for_game(gs_game)

        max_toi = int(toi_game["toi_sec"].max()) if not toi_game.empty else 0
        if max_toi > 3900:
            logger.warning("game_id=%s max player toi_sec=%s (>3900) after merge", game_id, max_toi)

        df_corsi = toi_game[["game_id", "player_id", "team_id"]].drop_duplicates().copy()
        df_corsi["cf"] = 0
        df_corsi["ca"] = 0

        df_ex = build_exclude_timeline_equal_strength(gs_game)
        gp_game_es = apply_exclude_to_plays(gp_game, df_ex)

        for _, ev in gp_game_es.iterrows():
            update_corsi_counts(df_corsi, ev, gs_game)

        merged = df_corsi.merge(toi_game, on=["game_id", "player_id", "team_id"], how="left")

        merged["cf60"] = np.where(
            merged["toi_sec"] > 0, merged["cf"] * 3600.0 / merged["toi_sec"], np.nan
        )
        merged["ca60"] = np.where(
            merged["toi_sec"] > 0, merged["ca"] * 3600.0 / merged["toi_sec"], np.nan
        )
        merged["cf_percent"] = np.where(
            (merged["cf"] + merged["ca"]) > 0,
            100.0 * merged["cf"] / (merged["cf"] + merged["ca"]),
            0.0,
        )

        out_rows.append(merged)

    if not out_rows:
        return pd.DataFrame()

    return pd.concat(out_rows, ignore_index=True)


def main() -> None:
    """Build mart.player_game_es_{season} for all modern seasons."""
    """Build mart.player_game_es_{season} for one or all modern seasons."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None, help="Run one season, e.g. 20182019")
    args = ap.parse_args()

    # choose seasons to run
    seasons = [args.season] if args.season is not None else [int(s) for s in SEASONS_MODERN]

    engine = get_db_engine()

    # for season in SEASONS_MODERN:
    for season in seasons:
        df = build_player_game_es_for_season(int(season))
        if df.empty:
            print(f"⚠️ {season}: no rows produced")
            continue

        out_table = f"player_game_es_{season}"
        schema = SCHEMA["mart"]

        df = df[
            ["game_id", "player_id", "team_id", "cf", "ca", "toi_sec", "cf60", "ca60", "cf_percent"]
        ].copy()

        # dtype cleanup (prevents surprises)
        df["game_id"] = df["game_id"].astype("int64")
        df["player_id"] = df["player_id"].astype("int64")
        df["team_id"] = df["team_id"].astype("int64")
        df["cf"] = df["cf"].astype("int64")
        df["ca"] = df["ca"].astype("int64")
        df["toi_sec"] = df["toi_sec"].astype("int64")

        # --- HARD GUARDRail: enforce unique key ---
        keys = ["game_id", "player_id", "team_id"]

        # If duplicates exist, collapse them deterministically (sum counts + sum toi)
        if df.duplicated(keys).any():
            dup_n = int(df.duplicated(keys).sum())
            logger.warning(
                "Found %s duplicate ES keys in season=%s; collapsing by sum()", dup_n, season
            )

            df = df.groupby(keys, as_index=False).agg({"cf": "sum", "ca": "sum", "toi_sec": "sum"})

            # recompute rate columns after aggregation
            df["cf60"] = np.where(df["toi_sec"] > 0, df["cf"] * 3600.0 / df["toi_sec"], np.nan)
            df["ca60"] = np.where(df["toi_sec"] > 0, df["ca"] * 3600.0 / df["toi_sec"], np.nan)
            df["cf_percent"] = np.where(
                (df["cf"] + df["ca"]) > 0,
                100.0 * df["cf"] / (df["cf"] + df["ca"]),
                0.0,
            )

        # Assert uniqueness (fail fast)
        assert not df.duplicated(keys).any(), f"ES still has dupes for season={season}"

        df["cf60"] = np.where(df["toi_sec"] > 0, df["cf"] * 3600.0 / df["toi_sec"], np.nan)
        df["ca60"] = np.where(df["toi_sec"] > 0, df["ca"] * 3600.0 / df["toi_sec"], np.nan)
        df["cf_percent"] = np.where(
            (df["cf"] + df["ca"]) > 0,
            100.0 * df["cf"] / (df["cf"] + df["ca"]),
            0.0,
        )

        df[["cf", "ca", "toi_sec"]] = df[["cf", "ca", "toi_sec"]].fillna(0)

        with engine.begin() as conn:
            mode = "replace"
            try:
                conn.execute(text(f'TRUNCATE TABLE "{schema}"."{out_table}"'))
                mode = "append"
            except ProgrammingError:
                pass

        df.to_sql(out_table, engine, schema=schema, if_exists=mode, index=False, method="multi")

        print(f"✅ wrote {len(df)} rows -> {schema}.{out_table}")

    engine.dispose()


if __name__ == "__main__":
    main()
