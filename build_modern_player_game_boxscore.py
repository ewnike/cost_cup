from __future__ import annotations

import os
import pathlib
import re
import unicodedata
from typing import Dict, Tuple

import pandas as pd
from sqlalchemy import text

from constants import SEASONS_MODERN
from db_utils import get_db_engine
from log_utils import setup_logger

logger = setup_logger()

RAW_SCHEMA = "raw"  # adjust if your raw pbp lives elsewhere
DERIVED_SCHEMA = "derived"
MART_SCHEMA = "mart"
EVENT_TYPES = ("GOAL", "SHOT", "MISS", "BLOCK", "HIT", "GIVE", "TAKE", "FAC", "PENL")

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")


def normalize_name(s: str) -> str:
    """Normalize player string for matching to player_info."""
    if not isinstance(s, str) or not s.strip():
        return ""
    s = s.strip()

    # normalize unicode accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    s = s.lower()
    s = s.replace(".", " ").replace("-", " ").replace("’", "'")

    # join Irish prefixes before punctuation->space (o'regan -> oregan)
    s = re.sub(r"\b([od])['’]", r"\1", s)

    # keep letters/spaces
    s = re.sub(r"[^a-z ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # collapse initials: "j t brown" -> "jt brown" (twice)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    return s


def build_player_lookup(engine) -> Dict[str, int]:
    """Map normalized 'first last' and 'f last' to player_id."""
    df = pd.read_sql_query(
        text('SELECT player_id, "firstName" AS first, "lastName" AS last FROM dim.player_info'),
        engine,
    )

    lookup: Dict[str, int] = {}
    for _, r in df.iterrows():
        first = normalize_name(str(r["first"]))
        last = normalize_name(str(r["last"]))
        if not first or not last:
            continue

        full = f"{first} {last}"
        init = f"{first[0]} {last}"

        pid = int(r["player_id"])
        lookup[full] = pid
        lookup[init] = pid

    return lookup


def build_game_player_team_map(engine, season: int) -> pd.DataFrame:
    """
    Reliable (game_id, player_id) -> team_id map from shifts.

    Uses raw.raw_shifts_resolved and dim_team_code.
    """
    q = text(
        f"""
        SELECT
          rs.game_id::bigint AS game_id,
          rs.player_id_resolved::bigint AS player_id,
          dt.team_id::bigint AS team_id
        FROM "{RAW_SCHEMA}"."raw_shifts_resolved" rs
        JOIN "dim"."dim_team_code" dt
          ON dt.team_code = rs.team
        WHERE rs.season = :season
          AND rs.session = 'R'
          AND rs.player_id_resolved IS NOT NULL
        """
    )
    df = pd.read_sql_query(q, engine, params={"season": int(season)})
    # choose most frequent team_id per player in a game (robust to weird rows)
    out = (
        df.groupby(["game_id", "player_id"])["team_id"]
        .agg(lambda x: x.value_counts().index[0])
        .reset_index()
    )
    return out


def build_team_id_for_against(df: pd.DataFrame, team_code_to_id: Dict[str, int]) -> pd.DataFrame:
    """
    Convert raw PBP team codes into team_id_for/team_id_against using home/away codes.

    event_team = acting team. opponent = the other of (home_team, away_team)
    """
    df = df.copy()
    df["team_id_for"] = df["event_team"].map(team_code_to_id)
    # opponent code
    opp = df.apply(
        lambda r: r["away_team"] if r["event_team"] == r["home_team"] else r["home_team"],
        axis=1,
    )
    df["team_id_against"] = opp.map(team_code_to_id)
    return df


def build_team_code_lookup(engine) -> Dict[str, int]:
    df = pd.read_sql_query(text("SELECT team_code, team_id FROM dim.dim_team_code"), engine)
    return {str(r["team_code"]): int(r["team_id"]) for _, r in df.iterrows()}


def build_player_game_boxscore_for_season(season: int) -> None:
    engine = get_db_engine()
    try:
        player_lookup = build_player_lookup(engine)
        team_code_to_id = build_team_code_lookup(engine)
        gp_team = build_game_player_team_map(engine, season)

        raw_table = f"raw_pbp_{season}"
        q = text(
            f"""
            SELECT
              season, game_id, event_index, game_period, game_seconds,
              event_type, event_team,
              event_player_1, event_player_2, event_player_3,
              home_team, away_team
            FROM "{RAW_SCHEMA}"."{raw_table}"
            WHERE season = :season
              AND session = 'R'
              AND event_type IN ('GOAL','SHOT','MISS','BLOCK','HIT','GIVE','TAKE','FAC','PENL')

            """
        )
        df = pd.read_sql_query(q, engine, params={"season": int(season)})

        if df.empty:
            logger.warning("%s: no pbp rows found", season)
            return

        df = build_team_id_for_against(df, team_code_to_id)

        # unpivot players 1/2/3 into long form
        long_rows = []
        for role_col, role in [
            ("event_player_1", "p1"),
            ("event_player_2", "p2"),
            ("event_player_3", "p3"),
        ]:
            tmp = df[
                [
                    "season",
                    "game_id",
                    "event_index",
                    "game_period",
                    "game_seconds",
                    "event_type",
                    "team_id_for",
                    "team_id_against",
                    role_col,
                ]
            ].copy()
            tmp = tmp.rename(columns={role_col: "player_raw"})
            tmp["role"] = role
            tmp = tmp[tmp["player_raw"].notna() & (tmp["player_raw"].astype(str).str.strip() != "")]
            long_rows.append(tmp)

        evp = pd.concat(long_rows, ignore_index=True)
        evp["player_key"] = evp["player_raw"].astype(str).map(normalize_name)

        # Map either "first last" or "f last" keys
        # If your raw uses "FIRST.LAST", normalize_name turns it into "first last" already.
        evp["player_id"] = evp["player_key"].map(player_lookup)

        # Drop unresolved (you can inspect these later)
        evp = evp.dropna(subset=["player_id"]).copy()
        evp["player_id"] = evp["player_id"].astype("int64")

        logger.info("%s: evp rows after player_id resolve=%s", season, len(evp))
        logger.info(
            "%s: evp event_type counts:\n%s", season, evp["event_type"].value_counts().to_string()
        )
        logger.info(
            "%s: FAC role counts:\n%s",
            season,
            evp.loc[evp["event_type"] == "FAC", "role"].value_counts().to_string(),
        )
        logger.info(
            "%s: BLOCK role counts:\n%s",
            season,
            evp.loc[evp["event_type"] == "BLOCK", "role"].value_counts().to_string(),
        )

        # Save event-player table (useful for forecasting / auditing)
        out_evp = f"pbp_event_players_{season}"
        evp.to_sql(
            out_evp, engine, schema=DERIVED_SCHEMA, if_exists="replace", index=False, method="multi"
        )
        logger.info("%s: wrote %s rows -> %s.%s", season, len(evp), DERIVED_SCHEMA, out_evp)

        # Build boxscore counts
        evp["goals"] = ((evp["event_type"] == "GOAL") & (evp["role"] == "p1")).astype(int)
        evp["assists"] = ((evp["event_type"] == "GOAL") & (evp["role"].isin(["p2", "p3"]))).astype(
            int
        )
        evp["shots"] = ((evp["event_type"].isin(["SHOT", "GOAL"])) & (evp["role"] == "p1")).astype(
            int
        )
        evp["hits"] = ((evp["event_type"] == "HIT") & (evp["role"] == "p1")).astype(int)

        ev = evp["event_type"]
        role = evp["role"]

        # BLOCK / TAKE / GIVE use short codes in your data
        evp["blocked"] = ((ev == "BLOCK") & (role == "p1")).astype(int)
        evp["takeaways"] = ((ev == "TAKE") & (role == "p1")).astype(int)
        evp["giveaways"] = ((ev == "GIVE") & (role == "p1")).astype(int)

        # Faceoffs: p1 winner, p2 loser (most likely in RTSS) — verify later if needed
        evp["faceoff_wins"] = ((ev == "FAC") & (role == "p1")).astype(int)
        evp["faceoff_taken"] = ((ev == "FAC") & (role.isin(["p1", "p2"]))).astype(int)

        # Penalties: p1 usually penalized; p2 often drawn-by
        evp["penalties_taken"] = ((ev == "PENL") & (role == "p1")).astype(int)

        box = evp.groupby(["season", "game_id", "player_id"], as_index=False)[
            [
                "goals",
                "assists",
                "shots",
                "hits",
                "blocked",
                "takeaways",
                "giveaways",
                "faceoff_wins",
                "faceoff_taken",
                "penalties_taken",
            ]
        ].sum()
        box["points"] = box["goals"] + box["assists"]

        # Attach team_id using shifts-derived mapping
        box = box.merge(gp_team, on=["game_id", "player_id"], how="left")
        box = box.dropna(subset=["team_id"]).copy()
        box["team_id"] = box["team_id"].astype("int64")

        out_box = f"player_game_boxscore_{season}"
        box.to_sql(
            out_box, engine, schema=MART_SCHEMA, if_exists="replace", index=False, method="multi"
        )
        logger.info("%s: wrote %s rows -> %s.%s", season, len(box), MART_SCHEMA, out_box)

    finally:
        engine.dispose()


def main() -> None:
    for season in SEASONS_MODERN:
        logger.info("Building modern boxscore for %s", season)
        build_player_game_boxscore_for_season(int(season))


if __name__ == "__main__":
    main()
