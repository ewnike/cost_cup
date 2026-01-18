import re

import requests
from sqlalchemy import create_engine, text

from db_utils import get_db_engine

ENGINE = get_db_engine()


BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
SEARCH_URL = "https://search.d3.nhle.com/api/v1/search/player"

TEAM_MAP = {
    "T.B": "TBL",
    "N.J": "NJD",
    "S.J": "SJS",
    "L.A": "LAK",
}


def norm_name(s) -> str:
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
    full = raw_to_full_name(raw_player)  # "Daniel O'Regan"
    parts = full.split()
    if len(parts) < 2:
        return norm_name(full)

    first_initial = parts[0][0].lower()
    last_token = norm_name(parts[-1]).replace(" ", "")  # O'Regan -> oregan
    return f"{first_initial} {last_token}"


def get_roster_candidates_from_boxscore(game_id: int):
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
    """Convert raw shifts name like 'JANIS.MOSER' -> 'Janis Moser'."""
    s = (raw_player or "").strip()
    s = re.sub(r"\.+", " ", s)  # dots -> spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s.title()


def raw_to_initial_key(raw_player: str) -> str:
    """Convert 'JANIS.MOSER' -> 'j moser' for matching abbreviated boxscore names."""
    full = raw_to_full_name(raw_player)
    parts = full.split()
    if len(parts) < 2:
        return norm_name(full)
    return f"{parts[0][0].lower()} {parts[-1].lower()}"


def main() -> None:
    # 1) Pull work items
    with ENGINE.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT name_key, raw_player, team, season, sample_game_id, n_rows
                FROM public.missing_player_work
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
