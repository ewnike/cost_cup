# caphit_playerid_backfill.py
from __future__ import annotations

import difflib
import re
import time
import unicodedata
from functools import lru_cache
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text

from constants import SCHEMA
from db_utils import get_db_engine

SEARCH_URL = "https://search.d3.nhle.com/api/v1/search/player"

NAME_ALIAS = {
    "zuccarello-aasen": "zuccarello",
    "grossman": "grossmann",
}


def py_norm_name(s: str) -> str:
    """Normalize names for matching across sources (handles accents, punctuation, initials)."""
    s = "" if s is None else str(s)
    s = s.strip().lower()

    # convert accents to ASCII: "bérubé" -> "berube"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

    # join Irish prefixes before punctuation->space: o'regan -> oregan, d'angelo -> dangelo
    s = re.sub(r"\b([od])['’]", r"\1", s)

    # punctuation -> space
    s = re.sub(r"[.\-’']", " ", s)

    # keep letters/spaces only
    s = re.sub(r"[^a-z ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()

    # collapse initials twice: "j t brown" -> "jt brown"
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    s = re.sub(r"\b([a-z])\s+([a-z])\b", r"\1\2", s)
    return s


def apply_alias(first: str, last: str) -> tuple[str, str]:
    # normalize + keep hyphen logic simple for alias keys
    k = py_norm_name(last).replace(" ", "-")
    if k in NAME_ALIAS:
        return first, NAME_ALIAS[k]
    return first, last


def _extract_hits(payload: Any):
    """
    Recursively search a JSON payload for a list of dicts that look like NHL player hits.
    Returns [] if nothing plausible found.
    """
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
    # sometimes split fields exist
    if not nm:
        fn = h.get("firstName") or h.get("first_name") or ""
        ln = h.get("lastName") or h.get("last_name") or ""
        nm = f"{fn} {ln}".strip()

    if isinstance(nm, dict):
        nm = nm.get("default") or nm.get("en") or next(iter(nm.values()), "")
    return str(nm)


def _score_name_match(target_full_norm: str, hit_full_norm: str) -> int:
    """
    Conservative scoring:
    - 100: exact normalized full name
    - 92: exact after removing spaces (helps "jeanluc" vs "jean luc")
    - 85: same last name + first name startswith (or matches initial)
    - 0: no acceptable match
    """
    if not target_full_norm or not hit_full_norm:
        return 0

    if hit_full_norm == target_full_norm:
        return 100

    if hit_full_norm.replace(" ", "") == target_full_norm.replace(" ", ""):
        return 92

    t_parts = target_full_norm.split()
    h_parts = hit_full_norm.split()
    if len(t_parts) < 2 or len(h_parts) < 2:
        return 0

    t_first, t_last = t_parts[0], t_parts[-1]
    h_first, h_last = h_parts[0], h_parts[-1]

    if h_last != t_last:
        return 0

    # first name exact / prefix / initial match
    if h_first == t_first:
        return 90
    if h_first.startswith(t_first) or t_first.startswith(h_first):
        return 88
    if (h_first[:1] == t_first[:1]) and (len(t_first) <= 2 or len(h_first) <= 2):
        # covers "jt" vs "j t" style collapses somewhat
        return 85

    return 0


def _split_first_last(full_name: str) -> tuple[str, str]:
    n = py_norm_name(full_name)
    parts = n.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _initials_from_first_parts(full_name: str) -> str:
    """
    Build initials from everything except last name:
      'pierre alexandre parenteau' -> 'pa'
      'jean francois berube' -> 'jf'
    """
    n = py_norm_name(full_name)
    parts = n.split()
    if len(parts) < 2:
        return ""
    first_parts = parts[:-1]  # drop last name
    initials = "".join(p[0] for p in first_parts if p)
    return initials


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

    # make it bidirectional
    out: dict[str, set[str]] = {}
    for formal, nicks in pairs.items():
        out.setdefault(formal, set()).update(nicks)
        for nick in nicks:
            out.setdefault(nick, set()).add(formal)
    return out


def _first_name_variants(first_norm: str, full_name: str | None = None) -> set[str]:
    """Nicknames + safe prefixes + initials for multi-part first names."""
    variants = {first_norm}

    # safe prefixes
    if len(first_norm) >= 4:
        variants.add(first_norm[:4])
    if len(first_norm) >= 3:
        variants.add(first_norm[:3])

    explicit = _bidirectional_nick_map()
    variants |= explicit.get(first_norm, set())

    # initials variant for multi-part first names: "pierre alexandre" -> "pa"
    if full_name:
        initials = _initials_from_first_parts(full_name)
        if initials and len(initials) >= 2:
            variants.add(initials)

    return variants


def _first_similarity(a: str, b: str) -> float:
    """
    Similarity with a strong rule:
    - if either is a prefix of the other (>=3 chars), treat as perfect (1.0)
      ex: chris vs christopher, nick vs nicholas
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    # prefix boost
    if len(a) >= 3 and (b.startswith(a) or a.startswith(b)):
        return 1.0

    return difflib.SequenceMatcher(None, a, b).ratio()


def _request_json(params: dict) -> dict | list:
    headers = {
        "User-Agent": "caphit-backfill/1.0 (+requests)",
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
    """
    Strategy:
      1) full-name query
      2) variant+last queries (Nick Paul, Jake Muzzin, PA Parenteau, etc.)
      3) last-name query (broad) + best-first selection
      4) last-name alternates (grossman -> grossmann)
    """
    target_first, target_last = _split_first_last(full_name)
    if not target_last:
        return None

    target_full_norm = py_norm_name(full_name)

    def _best_from_hits(
        hits: list[dict], full_name_for_variants: str
    ) -> tuple[float, int | None, str, list]:
        variants = _first_name_variants(target_first, full_name_for_variants)
        cands = []
        best = (-1.0, None, "")

        for h in hits:
            if not isinstance(h, dict):
                continue
            pid = _hit_player_id(h)
            if pid is None:
                continue
            hit_name = _hit_name(h)
            hit_norm = py_norm_name(hit_name)

            # exact full normalized match wins immediately (caller can short circuit)
            if hit_norm == target_full_norm or hit_norm.replace(
                " ", ""
            ) == target_full_norm.replace(" ", ""):
                return (1.5, pid, hit_name, [(1.5, pid, hit_name)])  # > 1.0 sentinel

            h_first, h_last = _split_first_last(hit_name)
            if h_last != target_last:
                continue

            sim = max(_first_similarity(v, h_first) for v in variants)
            cands.append((sim, pid, hit_name))

            if sim > best[0]:
                best = (sim, pid, hit_name)

        cands.sort(reverse=True, key=lambda x: x[0])
        return (best[0], best[1], best[2], cands)

    # 1) full-name query
    payload = _request_json({"culture": "en-us", "limit": 50, "q": full_name})
    hits = _extract_hits(payload)
    best_sim, best_pid, best_name, cands = _best_from_hits(hits, full_name)

    if best_sim > 1.0 and best_pid is not None:
        return best_pid

    if best_pid is not None and best_sim >= 0.92:
        return best_pid

    # 2) try variant + last name queries (fixes Nick Paul / Jake Muzzin / PA Parenteau)
    variants = _first_name_variants(target_first, full_name)
    # also include original first token from raw name, just in case normalization reduced it oddly
    for v in sorted(variants, key=len, reverse=True):
        q = f"{v} {target_last}".strip()
        payload_v = _request_json({"culture": "en-us", "limit": 50, "q": q})
        hits_v = _extract_hits(payload_v)
        sim_v, pid_v, name_v, cands_v = _best_from_hits(hits_v, full_name)

        if sim_v > 1.0 and pid_v is not None:
            print(
                f"DEBUG variant query exact '{full_name}' via q='{q}' -> '{name_v}' pid={pid_v}",
                flush=True,
            )
            return pid_v

        if pid_v is not None and sim_v >= 0.92:
            print(
                f"DEBUG variant query '{full_name}' via q='{q}' -> '{name_v}' sim={sim_v:.3f} pid={pid_v}",
                flush=True,
            )
            return pid_v

    # 3) broad last-name query (your existing fallback)
    payload2 = _request_json({"culture": "en-us", "limit": 200, "q": target_last})
    hits2 = _extract_hits(payload2)
    sim2, pid2, name2, cands2 = _best_from_hits(hits2, full_name)

    if not cands2:
        # 4) last-name alternates (common issue: Grossman vs Grossmann)
        alternates = []
        if target_last.endswith("man"):
            alternates.append(target_last + "n")  # grossman -> grossmann
        if target_last.endswith("son"):
            alternates.append(target_last[:-3] + "sen")  # sometimes (rare)

        for alt in alternates:
            payload_alt = _request_json({"culture": "en-us", "limit": 200, "q": alt})
            hits_alt = _extract_hits(payload_alt)
            sim_alt, pid_alt, name_alt, cands_alt = _best_from_hits(hits_alt, full_name)
            if cands_alt:
                # reuse acceptance logic below
                cands2 = cands_alt
                sim2, pid2, name2 = sim_alt, pid_alt, name_alt
                target_last = alt
                break

    if not cands2:
        print(f"DEBUG no last-name candidates for '{full_name}' (last='{target_last}')", flush=True)
        return None

    # acceptance:
    top_sim, top_pid, top_name = cands2[0]
    second_sim = cands2[1][0] if len(cands2) > 1 else -1.0

    # accept if strong
    if top_sim >= 0.80:
        print(
            f"DEBUG last-name fallback '{full_name}' -> '{top_name}' sim={top_sim:.3f} pid={top_pid} "
            f"(cands_top3={[(c[2], round(c[0], 3), c[1]) for c in cands2[:3]]})",
            flush=True,
        )
        return top_pid

    # accept if only candidate OR clear margin over #2
    if (len(cands2) == 1 and top_sim >= 0.55) or (
        top_sim >= 0.55 and (top_sim - second_sim) >= 0.20
    ):
        print(
            f"DEBUG relaxed accept '{full_name}' -> '{top_name}' sim={top_sim:.3f} pid={top_pid} "
            f"(second_sim={second_sim:.3f})",
            flush=True,
        )
        return top_pid

    print(
        f"DEBUG weak best match for '{full_name}': best='{top_name}' sim={top_sim:.3f} "
        f"(cands_top5={[(c[2], round(c[0], 3), c[1]) for c in cands2[:5]]})",
        flush=True,
    )
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


def backfill_season(season: int) -> None:
    engine = get_db_engine()
    table = f"{SCHEMA['dim']}.player_cap_hit_{season}"

    with engine.begin() as conn:
        missing = pd.read_sql(
            text(f"""
                SELECT "firstName", "lastName", spotrac_url
                FROM {table}
                WHERE player_id IS NULL
            """),
            conn,
        )

        if missing.empty:
            print(f"✅ {season}: no missing player_id")
            return

        resolved = 0
        skipped = 0

        for row in missing.itertuples(index=False):
            first = str(row.firstName).strip()
            last = str(row.lastName).strip()
            url = row.spotrac_url

            first2, last2 = apply_alias(first, last)
            full = f"{first2} {last2}".strip()

            try:
                pid = nhl_search_player_id(full)
            except Exception as e:
                print(f"⚠️ {season}: search error for {full}: {e}")
                skipped += 1
                continue

            if pid is None:
                print(f"❌ {season}: no match: {full}")
                skipped += 1
                continue

            upsert_dim_player_info(conn, pid, first, last)

            # update cap-hit row by spotrac_url when available
            if url is not None and str(url).strip() != "":
                conn.execute(
                    text(f"""
                        UPDATE {table}
                        SET player_id = :pid
                        WHERE spotrac_url = :url
                    """),
                    {"pid": pid, "url": url},
                )
            else:
                # fallback: update by name if no url (conservative)
                conn.execute(
                    text(f"""
                        UPDATE {table}
                        SET player_id = :pid
                        WHERE player_id IS NULL
                          AND "firstName" = :firstName
                          AND "lastName"  = :lastName
                    """),
                    {"pid": pid, "firstName": first, "lastName": last},
                )

            resolved += 1
            time.sleep(0.08)  # be polite

        print(f"✅ {season}: resolved={resolved}, skipped={skipped}")

    engine.dispose()


if __name__ == "__main__":
    for s in (20152016, 20162017, 20172018):
        backfill_season(s)
