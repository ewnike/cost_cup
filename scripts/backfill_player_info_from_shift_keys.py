"""
Backfill dim.player_info using missing normalized name_keys derived from raw shifts.

This reuses the same NHL search + conservative matching logic as caphit_playerid_backfill.py,
but instead of reading cap-hit tables, it reads a provided list of name_keys.

Run:
  python scripts/backfill_player_info_from_shift_keys.py
"""

from __future__ import annotations

import difflib
import re
import time
import unicodedata
from typing import Any

import requests
from sqlalchemy import text

from constants import SCHEMA
from db_utils import get_db_engine

SEARCH_URL = "https://search.d3.nhle.com/api/v1/search/player"

# --- Paste your missing keys here ---
MISSING_NAME_KEYS: list[str] = [
    "aleksei kolosov",
    "alex alexeyev",
    "alexandar georgiev",
    "alex barabanov",
    "alex chmelevski",
    "alex debrincat",
    "alex edler",
    "alex holtz",
    "alexis lafreniere",
    "alex kerfoot",
    "alex killorn",
    "alex nylander",
    "alex radulov",
    "alex romanov",
    "alex steen",
    "alex texier",
    "alex volkov",
    "alex wennberg",
    "arvid soderblom",
    "calvin petersen",
    "cam atkinson",
    "cam talbot",
    "cam york",
    "dan vladar",
    "egor zamula",
    "erik kallgren",
    "ethen frank",
    "evgeny dadonov",
    "fedor svechkov",
    "fredrik olofsson",
    "gabriel vilardi",
    "gaetan haas",
    "jake christiansen",
    "jake muzzin",
    "jakob forsbacka karlsson",
    "jesse ylonen",
    "jj peterka",
    "josh anderson",
    "josh brown",
    "josh mahura",
    "josh morrissey",
    "kenny agostino",
    "louie belpedio",
    "matej blumel",
    "mats zuccarello",
    "matt benning",
    "matt boldy",
    "matt coronato",
    "matt dumba",
    "matt grzelcyk",
    "matt murray",
    "matt rempe",
    "matty beniers",
    "max comtois",
    "mike matheson",
    "mitch marner",
    "nick caamano",
    "nick paul",
    "nick ritchie",
    "nicolas meloche",
    "nic petan",
    "nikolai prokhorkin",
    "nils aman",
    "philippe maillet",
    "philipp grubauer",
    "phil kessel",
    "sam montembeault",
    "sammy blais",
    "sam poulin",
    "tim stutzle",
    "tim washe",
    "tom wilson",
    "tony deangelo",
    "vasily ponomarev",
    "vinnie hinostroza",
    "zach fucale",
    "zach werenski",
    "zac jones",
]

# ----------------- name normalization (same spirit as your caphit script) -----------------


def py_norm_name(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\b([od])['’]", r"\1", s)  # o'regan -> oregan
    s = re.sub(r"[.\-’']", " ", s)  # punctuation -> space
    s = re.sub(r"[^a-z ]", "", s)  # keep letters/spaces only
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    return s


def _split_first_last(full_name: str) -> tuple[str, str]:
    n = py_norm_name(full_name)
    parts = n.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _bidirectional_nick_map() -> dict[str, set[str]]:
    pairs = {
        "michael": {"mike"},
        "nicholas": {"nick"},
        "christopher": {"chris"},
        "jacob": {"jake"},
        "alexander": {"alex", "sasha"},
        "matthew": {"matt"},
        "jonathan": {"jon", "john"},
        "william": {"will", "bill"},
        "anthony": {"tony"},
        "joseph": {"joe"},
        "patrick": {"pat"},
        "timothy": {"tim"},
        "benjamin": {"ben"},
    }
    out: dict[str, set[str]] = {}
    for formal, nicks in pairs.items():
        out.setdefault(formal, set()).update(nicks)
        for nick in nicks:
            out.setdefault(nick, set()).add(formal)
    return out


def _first_name_variants(first_norm: str) -> set[str]:
    variants = {first_norm}
    if len(first_norm) >= 4:
        variants.add(first_norm[:4])
    if len(first_norm) >= 3:
        variants.add(first_norm[:3])
    variants |= _bidirectional_nick_map().get(first_norm, set())
    return variants


def _first_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if len(a) >= 3 and (b.startswith(a) or a.startswith(b)):
        return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def _extract_hits(payload: Any):
    if isinstance(payload, list):
        if payload and isinstance(payload[0], dict):
            keys = payload[0].keys()
            if ("playerId" in keys or "id" in keys) and ("name" in keys or "fullName" in keys):
                return payload
        for item in payload:
            hits = _extract_hits(item)
            if hits:
                return hits
        return []
    if isinstance(payload, dict):
        for k in ("data", "results", "items", "players", "docs", "hits"):
            if k in payload:
                hits = _extract_hits(payload[k])
                if hits:
                    return hits
        for v in payload.values():
            hits = _extract_hits(v)
            if hits:
                return hits
    return []


def _hit_player_id(h: dict) -> int | None:
    pid = h.get("playerId") or h.get("player_id") or h.get("id")
    try:
        return int(pid) if pid is not None else None
    except Exception:
        return None


def _hit_name(h: dict) -> str:
    nm = h.get("name") or h.get("fullName") or h.get("full_name") or ""
    if not nm:
        fn = h.get("firstName") or h.get("first_name") or ""
        ln = h.get("lastName") or h.get("last_name") or ""
        nm = f"{fn} {ln}".strip()
    if isinstance(nm, dict):
        nm = nm.get("default") or nm.get("en") or next(iter(nm.values()), "")
    return str(nm)


def _request_json(params: dict) -> dict | list:
    headers = {
        "User-Agent": "shift-playerinfo-backfill/1.0 (+requests)",
        "Accept": "application/json",
    }
    for attempt in range(1, 4):
        r = requests.get(SEARCH_URL, params=params, headers=headers, timeout=20)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.6 * attempt)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return {}


def nhl_search_player_id(full_name: str) -> int | None:
    target_first, target_last = _split_first_last(full_name)
    if not target_last:
        return None
    target_full_norm = py_norm_name(full_name)

    def best_from_hits(hits: list[dict]) -> tuple[float, int | None, str]:
        variants = _first_name_variants(target_first)
        best = (-1.0, None, "")
        for h in hits:
            if not isinstance(h, dict):
                continue
            pid = _hit_player_id(h)
            if pid is None:
                continue
            hit_name = _hit_name(h)
            hit_norm = py_norm_name(hit_name)

            if hit_norm == target_full_norm or hit_norm.replace(
                " ", ""
            ) == target_full_norm.replace(" ", ""):
                return (1.5, pid, hit_name)

            h_first, h_last = _split_first_last(hit_name)
            if h_last != target_last:
                continue

            sim = max(_first_similarity(v, h_first) for v in variants)
            if sim > best[0]:
                best = (sim, pid, hit_name)
        return best

    # full-name query
    payload = _request_json({"culture": "en-us", "limit": 50, "q": full_name})
    hits = _extract_hits(payload)
    sim, pid, _ = best_from_hits(hits)
    if sim > 1.0 and pid is not None:
        return pid
    if pid is not None and sim >= 0.92:
        return pid

    # last-name query
    payload2 = _request_json({"culture": "en-us", "limit": 200, "q": target_last})
    hits2 = _extract_hits(payload2)
    sim2, pid2, _ = best_from_hits(hits2)
    if pid2 is not None and sim2 >= 0.80:
        return pid2
    if pid2 is not None and sim2 >= 0.55:
        return pid2
    return None


def upsert_dim_player_info(conn, player_id: int, first: str, last: str) -> None:
    conn.execute(
        text(f"""
            INSERT INTO {SCHEMA["dim"]}.player_info (player_id, "firstName", "lastName")
            VALUES (:player_id, :firstName, :lastName)
            ON CONFLICT (player_id) DO UPDATE
            SET "firstName" = EXCLUDED."firstName",
                "lastName"  = EXCLUDED."lastName"
        """),
        {"player_id": player_id, "firstName": first, "lastName": last},
    )


def pretty_query_name_from_key(name_key: str) -> str:
    # best-effort: NHL search is forgiving; this is just to form the query string
    return " ".join(w.capitalize() for w in name_key.split())


def main() -> None:
    engine = get_db_engine()

    resolved = 0
    skipped = 0
    still_missing: list[str] = []

    with engine.begin() as conn:
        for nk in MISSING_NAME_KEYS:
            nk_norm = py_norm_name(nk)
            qname = pretty_query_name_from_key(nk_norm)

            pid = None
            try:
                pid = nhl_search_player_id(qname)
            except Exception as e:
                print(f"⚠️ search error for '{nk}': {e}", flush=True)
                skipped += 1
                still_missing.append(nk)
                continue

            if pid is None:
                print(f"❌ no match for '{nk}' (q='{qname}')", flush=True)
                skipped += 1
                still_missing.append(nk)
                continue

            # store best-effort first/last from the key (you can later enrich from NHL API if desired)
            parts = nk_norm.split()
            first = parts[0].capitalize() if parts else ""
            last = parts[-1].capitalize() if len(parts) >= 2 else ""

            upsert_dim_player_info(conn, pid, first, last)
            resolved += 1

            print(f"✅ {nk} -> pid={pid}", flush=True)
            time.sleep(0.08)

    engine.dispose()

    print("\n----- SUMMARY -----")
    print(f"resolved={resolved}, skipped={skipped}")
    if still_missing:
        print("\nStill missing:")
        for nk in still_missing:
            print(f"  - {nk}")


if __name__ == "__main__":
    main()
