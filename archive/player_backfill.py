"""
Deprecated code replaced by golden sql statement.

Resolve missing NHL player IDs from raw shift/boxscore name strings and upsert them into Postgres.

This script reads unresolved player rows from depr. `missing_player_work`, attempts to match each
raw player name to an NHL `playerId` using the NHL gamecenter boxscore endpoint (preferred), and
falls back to the NHL player search endpoint when needed. Successful matches are upserted into
`dim.player_info`.

Matching strategy (in order):
  1) Normalize full name and match against normalized boxscore roster names.
  2) Match using first-initial + normalized last-name key (handles abbreviated boxscore forms).
  3) Last-chance substring heuristic on the normalized keys.
  4) Fallback to NHL search API with progressively looser matching rules.

Notes:
  - Requires network access to NHL APIs.
  - Requires database connectivity via `db_utils.get_db_engine()`.
  - Designed to be run as a one-off maintenance job; prints a summary of unresolved rows.

"""

import os
import pathlib
import re

import requests
from sqlalchemy import text

from db_utils import get_db_engine

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

# pylint: disable=duplicate-code

ENGINE = get_db_engine()


BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
SEARCH_URL = "https://search.d3.nhle.com/api/v1/search/player"

TEAM_MAP = {
    "T.B": "TBL",
    "N.J": "NJD",
    "S.J": "SJS",
    "L.A": "LAK",
}

raise SystemExit(
    "DEPRECATED: This script uses legacy Python name normalization. "
    "Use scripts/backfill_player_info_from_shift_keys.py or the golden SQL pipeline."
)


def norm_name(s) -> str:
    """
    Normalize a player name for matching.

    Lowercases, removes punctuation/diacritics-like characters, collapses whitespace,
    joins common Irish prefixes (e.g., O'Regan -> oregan), and collapses spaced initials
    (e.g., "j t brown" -> "jt brown").

    Args:
        s: Input name (string-like). None/other types are coerced to str.

    Returns:
        A normalized, ASCII a–z + space string suitable for fuzzy matching.

    """
    if s is None:
        s = ""
    if not isinstance(s, str):
        s = str(s)

    s = s.strip().lower()

    # ✅ join Irish prefixes before punctuation->space
    # e.g. "o'regan" -> "oregan", "d'angelo" -> "dangelo"
    s = re.sub(r"\b([od])['’]", r"\1", s)

    s = re.sub(r"[.\-’']", " ", s)
    s = re.sub(r"[^a-z ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()

    # collapse initials: "j t brown" -> "jt brown" (do twice)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    return s


def raw_to_initial_key(raw_player: str) -> str:
    """
    Build an initial+last-name key from a raw player string.

    Example:
        "JANIS.MOSER" -> "j moser"
        "Daniel O'Regan" -> "d oregan"

    This is useful because boxscores often abbreviate first names to an initial.

    Args:
        raw_player: Raw name string (often dot-separated) from shifts/work table.

    Returns:
        A string of the form "<first_initial> <normalized_last_name>".

    """
    full = raw_to_full_name(raw_player)
    parts = full.split()
    if len(parts) < 2:
        return norm_name(full)

    first_initial = parts[0][0].lower()
    last_token = norm_name(parts[-1]).replace(" ", "")
    return f"{first_initial} {last_token}"


def get_roster_candidates_from_boxscore(game_id: int):
    """
    Fetch roster player candidates from the NHL gamecenter boxscore endpoint.

    Args:
        game_id: NHL game ID used to query the boxscore endpoint.

    Returns:
        A list of tuples: (player_id, normalized_name, display_name).
        The normalized name is produced by `norm_name()` and is intended for matching.

    Raises:
        requests.HTTPError: If the boxscore request fails.
        requests.RequestException: For network/timeout-related errors.

    """
    url = BOX_URL.format(game_id=game_id)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    candidates = {}

    pbs = data.get("playerByGameStats", {})
    for side in ("homeTeam", "awayTeam"):
        team_obj = pbs.get(side, {}) or {}
        for group in ("forwards", "defense", "goalies"):
            for p in team_obj.get(group, []) or []:
                pid = p.get("playerId") or p.get("id")

                name = p.get("name")
                # ✅ handle dict form: {"default": "..."}
                if isinstance(name, dict):
                    name = name.get("default")

                if pid and isinstance(name, str) and name.strip():
                    candidates[int(pid)] = (norm_name(name), name.strip())

    return [(pid, v[0], v[1]) for pid, v in candidates.items()]


def fallback_search_player_id(name_key: str):
    """
    Best-effort player ID lookup using the NHL player search API.

    Queries `SEARCH_URL` with `q=name_key` and supports multiple response shapes
    (either a list or a dict containing 'data'/'results'). Attempts matching in order:
      1) Exact match: norm_name(full_name) == name_key
      2) Last-name match + first-initial match
      3) If exactly one hit is returned, accept it

    Args:
        name_key: Normalized name string used for search and comparison.

    Returns:
        Player ID as int if resolved, otherwise None.

    Raises:
        requests.HTTPError: If the HTTP request returns a non-success status code.
        requests.RequestException: For network/timeout-related errors.

    """
    params = {"culture": "en-us", "limit": 50, "q": name_key}
    r = requests.get(SEARCH_URL, params=params, timeout=20)
    r.raise_for_status()
    payload = r.json()

    hits = (
        payload
        if isinstance(payload, list)
        else (payload.get("data") or payload.get("results") or [])
    )

    target = name_key
    target_parts = target.split()
    target_last = target_parts[-1] if target_parts else ""
    target_first_initial = target_parts[0][0] if target_parts and target_parts[0] else ""

    # 1) exact normalized match
    for h in hits:
        pid = h.get("playerId") or h.get("id")
        full = h.get("name") or h.get("fullName") or ""
        hk = norm_name(full)
        if pid and hk == target:
            return int(pid)

    # 2) last name match + first initial match
    for h in hits:
        pid = h.get("playerId") or h.get("id")
        full = h.get("name") or h.get("fullName") or ""
        hk = norm_name(full)
        if not pid or not hk:
            continue
        hk_parts = hk.split()
        if not hk_parts:
            continue
        if hk_parts[-1] == target_last and hk_parts[0][0] == target_first_initial:
            return int(pid)

    # 3) if only one hit, accept it
    if len(hits) == 1:
        h = hits[0]
        pid = h.get("playerId") or h.get("id")
        if pid:
            return int(pid)

    return None


def upsert_player_info(conn, player_id: int, full_name: str):
    """
    Upsert a player's name into dim.player_info by player_id.

    Inserts (player_id, firstName, lastName) into `dim.player_info` and updates on conflict.
    Note: this function currently derives `firstName` as the first token of `full_name` and
    stores the entire `full_name` string in `lastName`.

    Args:
        conn: An active SQLAlchemy connection/transaction.
        player_id: NHL player ID.
        full_name: Player's full name (e.g., "Janis Moser").

    Returns:
        None.

    """
    parts = full_name.split(" ", 1)
    first = parts[0].strip() if parts else None
    last = full_name.strip()

    conn.execute(
        text("""
        INSERT INTO dim.player_info (player_id, "firstName", "lastName")
        VALUES (:player_id, :firstName, :lastName)
        ON CONFLICT (player_id) DO UPDATE
        SET "firstName" = EXCLUDED."firstName",
            "lastName"  = EXCLUDED."lastName";
    """),
        {"player_id": player_id, "firstName": first, "lastName": last},
    )


def raw_to_full_name(raw_player: str) -> str:
    """
    Convert a raw shifts-style name into title case.

    Converts dot-separated or oddly spaced names into a display form.
    Example: "JANIS.MOSER" -> "Janis Moser".

    Args:
        raw_player: Raw name string.

    Returns:
        Title-cased name with dots collapsed to spaces.

    """
    s = (raw_player or "").strip()
    s = re.sub(r"\.+", " ", s)  # dots -> spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s.title()


def main() -> None:
    """
    Run the end-to-end resolution job.

    Reads missing player rows from `missing_player_work`, resolves player IDs via
    boxscore/search endpoints, upserts results into `dim.player_info`, and prints a summary.
    """
    # 1) Pull work items
    with ENGINE.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT name_key, raw_player, team, season, sample_game_id, n_rows
                FROM missing_player_work
                ORDER BY n_rows DESC
                """
            )
        ).fetchall()

    resolved = 0
    unresolved: list[tuple] = []

    # 2) Resolve + upsert
    with ENGINE.begin() as conn:
        for name_key, raw_player, team, season, game_id, n_rows in rows:
            target_full_key = norm_name(raw_to_full_name(raw_player))  # "janis moser"
            target_init_key = raw_to_initial_key(raw_player)  # "j moser"

            try:
                candidates = get_roster_candidates_from_boxscore(int(game_id))
            except Exception as e:
                unresolved.append((raw_player, team, season, game_id, f"boxscore_error: {e}"))
                continue

            # Prefer full-name match; then initial+last match (boxscore often uses "J. Moser")
            match = [c for c in candidates if c[1] == target_full_key]
            if not match:
                match = [c for c in candidates if c[1] == target_init_key]

            # Last-chance heuristic (rarely needed, but can help with spacing quirks)
            if not match:
                match = [
                    c for c in candidates if target_init_key in c[1] or c[1] in target_init_key
                ]

            if len(match) == 1:
                pid, _, _abbr_name = match[0]
                upsert_player_info(conn, int(pid), raw_to_full_name(raw_player))
                resolved += 1
                continue

            if len(match) > 1:
                unresolved.append(
                    (
                        raw_player,
                        team,
                        season,
                        game_id,
                        f"multiple_matches: {[m[2] for m in match]}",
                    )
                )
                continue

            # Fallback: search endpoint (use full-name key)
            pid = fallback_search_player_id(target_full_key)
            if pid:
                upsert_player_info(conn, int(pid), raw_to_full_name(raw_player))
                resolved += 1
            else:
                unresolved.append((raw_player, team, season, game_id, "no_match"))

    # 3) Output summary (no extra boxscore debug spam)
    print(f"Resolved players inserted/updated: {resolved}")
    if unresolved:
        print("Unresolved (first 50):")
        for u in unresolved[:50]:
            print("  ", u)

    ENGINE.dispose()


if __name__ == "__main__":
    main()
