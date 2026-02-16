"""
Backfill missing NHL `player_id` values in Spotrac cap-hit tables.

For each season table `dim.player_cap_hit_{season}`, the script selects rows where `player_id`
is NULL, attempts to resolve the NHL player ID via the NHL search API, and updates both:
  - `dim.player_info` (upsert of player_id, firstName, lastName)
  - the cap-hit season table (sets player_id), preferring updates by `spotrac_url` when present

The name resolution uses normalized string matching, nicknames/prefix variants, and a conservative
acceptance policy to reduce false matches.

Notes:
  - Requires network access to `SEARCH_URL`.
  - Requires database connectivity via `db_utils.get_db_engine()`.

"""

from __future__ import annotations

import difflib
import os
import pathlib
import re
import time
import unicodedata
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text

from constants import SCHEMA
from db_utils import get_db_engine

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")

# pylint: disable=duplicate-code

SEARCH_URL = "https://search.d3.nhle.com/api/v1/search/player"

NAME_ALIAS = {
    "zuccarello-aasen": "zuccarello",
    "grossman": "grossmann",
    "blueger": "blueger",
}

FIRST_ALIAS_BY_LAST = {
    "blueger": {"theodor": "teddy"},
}

SPOTRAC_ID_TO_NHL_ID = {
    19956: 8478427,  # CAR forward
    24067: 8480222,  # NYI defenseman
}

_SPOTRAC_ID_RE = re.compile(r"/(\d+)(?=(/|$|\?|#))")


def lookup_nhl_id_from_spotrac(conn, map_table: str, spotrac_id: int) -> int | None:
    """
    Docstring for lookup_nhl_id_from_spotrac.

    :param conn: Description
    :param map_table: Description
    :type map_table: str
    :param spotrac_id: Description
    :type spotrac_id: int
    :return: Description
    :rtype: int | None
    """
    row = conn.execute(
        text(f"""
            SELECT player_id
            FROM {map_table}
            WHERE spotrac_id = :sid
        """),
        {"sid": int(spotrac_id)},
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


# def upsert_spotrac_to_nhl(
#     conn,
#     spotrac_id: int,
#     player_id: int,
#     confidence: str,
#     source: str,
#     map_table: str | None = None,
# ) -> None:
#     """
#     Docstring for upsert_spotrac_to_nhl.

#     :param conn: Description
#     :param spotrac_id: Description
#     :type spotrac_id: int
#     :param player_id: Description
#     :type player_id: int
#     :param confidence: Description
#     :type confidence: str
#     :param source: Description
#     :type source: str
#     :param map_table: Description
#     :type map_table: str | None
#     """
#     map_table = map_table or f"{SCHEMA['dim']}.spotrac_to_nhl"


#     conn.execute(
#         text(f"""
#             INSERT INTO {map_table} (spotrac_id, player_id, confidence, source, created_at)
#             VALUES (:sid, :pid, :conf, :src, NOW())
#             ON CONFLICT (spotrac_id) DO UPDATE
#               SET player_id  = EXCLUDED.player_id,
#                   confidence = EXCLUDED.confidence,
#                   source     = EXCLUDED.source
#             WHERE
#               -- only update if this player_id isn't already mapped to another spotrac_id
#               NOT EXISTS (
#                 SELECT 1
#                 FROM {map_table} m
#                 WHERE m.player_id = EXCLUDED.player_id
#                   AND m.spotrac_id <> EXCLUDED.spotrac_id
#               )
#         """),
#         {"sid": int(spotrac_id), "pid": int(player_id), "conf": confidence, "src": source},
#     )
def upsert_spotrac_to_nhl(
    conn,
    map_table: str,
    spotrac_id: int,
    player_id: int,
    confidence: str,
    source: str,
) -> bool:
    """
    Upsert spotrac_id -> player_id into dim.spotrac_to_nhl.

    Returns:
        True if inserted/updated, False if skipped due to player_id collision.

    """
    # If this player_id is already mapped to another spotrac_id, skip
    row = conn.execute(
        text(f"""
            SELECT spotrac_id
            FROM {map_table}
            WHERE player_id = :pid
              AND spotrac_id <> :sid
        """),
        {"pid": int(player_id), "sid": int(spotrac_id)},
    ).fetchone()

    if row:
        existing_sid = int(row[0])
        print(
            f"⚠️ spotrac_to_nhl collision: player_id={player_id} already mapped "
            f"to spotrac_id={existing_sid}; skipping new spotrac_id={spotrac_id}",
            flush=True,
        )
        return False

    # Otherwise safe upsert by spotrac_id
    conn.execute(
        text(f"""
            INSERT INTO {map_table} (spotrac_id, player_id, confidence, source, created_at)
            VALUES (:sid, :pid, :conf, :src, NOW())
            ON CONFLICT (spotrac_id)
            DO UPDATE SET
                player_id  = EXCLUDED.player_id,
                confidence = EXCLUDED.confidence,
                source     = EXCLUDED.source
        """),
        {"sid": int(spotrac_id), "pid": int(player_id), "conf": confidence, "src": source},
    )
    return True


def spotrac_id_from_url(url: str) -> int | None:
    """
    Docstring for spotrac_id_from_url.

    :param url: Description
    :type url: str
    :return: Description
    :rtype: int | None
    """
    if not url:
        return None
    m = _SPOTRAC_ID_RE.search(url)
    return int(m.group(1)) if m else None


def py_norm_name(s: str) -> str:
    """
    Normalize a name for cross-source matching.

    Lowercases, removes accents, converts punctuation to spaces, strips non-letters,
    collapses whitespace, joins Irish prefixes (O'/D'), and collapses spaced initials.

    Returns a stable ASCII a–z + space representation.
    """
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
    """
    Docstring for apply_alias.

    :param first: Description
    :type first: str
    :param last: Description
    :type last: str
    :return: Description
    :rtype: tuple[str, str]
    """
    last_norm = py_norm_name(last)
    first_norm = py_norm_name(first)

    # last-name alias
    k_last = last_norm.replace(" ", "-")
    if k_last in NAME_ALIAS:
        last = NAME_ALIAS[k_last]
        last_norm = py_norm_name(last)

    # first-name alias gated by last
    if last_norm in FIRST_ALIAS_BY_LAST:
        first = FIRST_ALIAS_BY_LAST[last_norm].get(first_norm, first)

    return first, last


def _extract_hits(payload: Any):
    """
    Recursively search a JSON payload for a plausible list of NHL player result dicts.

    A "hit" list is identified as a list of dicts containing an ID field (playerId/id)
    and a name field (name/fullName). Returns [] if nothing plausible is found.
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
    """Extract an integer player ID from a search hit dict."""
    pid = h.get("playerId") or h.get("player_id") or h.get("id")
    try:
        return int(pid) if pid is not None else None
    except Exception:
        return None


def _hit_name(h: dict) -> str:
    """
    Extract a displayable full name from a search hit dict.

    Supports 'name'/'fullName' fields, optional split first/last fields, and localized dict forms.
    """
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
    Score a name match using conservative, rule-based thresholds.

    Returns:
        100: exact normalized full name
         92: exact after removing spaces (e.g., "jeanluc" vs "jean luc")
         90: same last name and exact first name
         88: same last name and first-name prefix match (either direction)
         85: same last name and initial-style match for short first tokens
          0: no acceptable match

    """
    # pylint: disable=unused-function
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
    """
    Normalize and split a full name into (first, last) tokens.

    Uses the first token as first name and the last token as last name.
    """
    n = py_norm_name(full_name)
    parts = n.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _initials_from_first_parts(full_name: str) -> str:
    """
    Build initials from everything except last name.

      'pierre alexandre parenteau' -> 'pa'
      'jean francois berube' -> 'jf'.
    """
    n = py_norm_name(full_name)
    parts = n.split()
    if len(parts) < 2:
        return ""
    first_parts = parts[:-1]  # drop last name
    initials = "".join(p[0] for p in first_parts if p)
    return initials


def build_nickname_cliques() -> dict[str, set[str]]:
    """
    Build a fully-connected nickname map.

    If alexander has {alex, sasha}, then:
      alexander -> {alex, sasha}
      alex      -> {alexander, sasha}
      sasha     -> {alexander, alex}
    """
    buckets = {
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
        "cristovel": {"boo"},
    }

    out: dict[str, set[str]] = {}

    for formal, nicks in buckets.items():
        bucket = {formal, *nicks}
        for name in bucket:
            out[name] = bucket - {name}  # everyone else in the same bucket

    return out


def _bidirectional_nick_map() -> dict[str, set[str]]:
    return build_nickname_cliques()


def _first_name_variants(first_norm: str, full_name: str | None = None) -> set[str]:
    """
    Generate acceptable variants for matching first names.

    Includes the normalized first name, short safe prefixes (3–4 chars), nickname equivalents,
    and (optionally) multi-part initials (e.g., "pierre alexandre" -> "pa").
    """
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
    Similarity with a strong rule.

    - if either is a prefix of the other (>=3 chars), treat as perfect (1.0)
      ex: chris vs christopher, nick vs nicholas.
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
    """
    Call the NHL player search API with retries for transient errors.

    Retries up to 3 times on common transient statuses (429, 5xx) with short backoff.

    Raises:
        requests.HTTPError / RequestException on permanent failures.

    """
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
    Resolve an NHL player ID from a full name using the NHL search API.

    Approach:
      1) Query using the full name; accept exact/near-exact normalized matches.
      2) Query using first-name variants + last name (nicknames, prefixes, multi-part initials).
      3) Broad last-name query and select the best first-name similarity among same-last hits.
      4) Try common last-name alternates (e.g., grossman -> grossmann) if no candidates found.

    Returns:
        player_id if a sufficiently strong match is found, otherwise None.

    """
    target_first, target_last = _split_first_last(full_name)
    if not target_last:
        return None

    target_full_norm = py_norm_name(full_name)

    def _best_from_hits(
        hits: list[dict], full_name_for_variants: str
    ) -> tuple[float, int | None, str, list[tuple[float, int, str]]]:
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
    best_sim, best_pid, _, _ = _best_from_hits(hits, full_name)

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
                f"DEBUG variant query '{full_name}' via q='{q}' -> '{name_v}' "
                f"sim={sim_v:.3f} pid={pid_v}",
                flush=True,
            )
            return pid_v

    # 3) broad last-name query (your existing fallback)
    payload2 = _request_json({"culture": "en-us", "limit": 200, "q": target_last})
    hits2 = _extract_hits(payload2)
    _, _, _, cands2 = _best_from_hits(hits2, full_name)

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
                _, _, _ = sim_alt, pid_alt, name_alt
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
        top3 = [(c[2], round(c[0], 3), c[1]) for c in cands2[:3]]
        print(
            (
                f"DEBUG last-name fallback '{full_name}' -> '{top_name}' "
                f"sim={top_sim:.3f} pid={top_pid} "
                f"(cands_top3={top3})"
            ),
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
    """Upsert a row into dim.player_info for the given NHL player_id."""
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


def discover_cap_hit_seasons() -> list[int]:
    """Return seasons that have a dim.player_cap_hit_{season} table."""
    engine = get_db_engine()
    with engine.begin() as conn:
        rows = conn.execute(
            text(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA["dim"]}'
              AND table_name LIKE 'player_cap_hit_%'
        """)
        ).fetchall()
    engine.dispose()

    seasons = []
    for (tname,) in rows:
        m = re.match(r"player_cap_hit_(\d{8})$", tname)
        if m:
            seasons.append(int(m.group(1)))
    return sorted(seasons)


def backfill_season(season: int) -> None:
    """
    Backfill dim.player_cap_hit_{season}.player_id using.

      1) dim.spotrac_to_nhl lookup by parsed spotrac_id from spotrac_url
      2) NHL search fallback (name-based) if mapping missing

    When NHL search resolves and spotrac_id exists, upsert the mapping:
      spotrac_id -> player_id.
    """
    engine = get_db_engine()
    cap_table = f"{SCHEMA['dim']}.player_cap_hit_{season}"
    map_table = f"{SCHEMA['dim']}.spotrac_to_nhl"

    with engine.begin() as conn:
        # 0) Pull caphit rows that still need player_id
        missing = pd.read_sql(
            text(f"""
                SELECT
                    player_id,
                    "firstName",
                    "lastName",
                    "capHit",
                    spotrac_url
                FROM {cap_table}
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
            first = str(row.firstName).strip() if row.firstName is not None else ""
            last = str(row.lastName).strip() if row.lastName is not None else ""
            url = str(row.spotrac_url).strip() if row.spotrac_url is not None else ""

            # must have URL to update deterministically (and you have UNIQUE(spotrac_url))
            if not url:
                print(f"⚠️ {season}: missing spotrac_url for '{first} {last}'; skipping")
                skipped += 1
                continue

            first2, last2 = apply_alias(first, last)
            full = f"{first2} {last2}".strip()

            spotrac_id = spotrac_id_from_url(url)
            pid: int | None = None

            # 1) Spotrac→NHL mapping lookup first
            if spotrac_id is not None:
                # pid = lookup_nhl_id_from_spotrac(conn, spotrac_id, map_table=map_table)
                pid = lookup_nhl_id_from_spotrac(conn, map_table, spotrac_id)
            # 2) NHL search fallback
            if pid is None:
                try:
                    pid = nhl_search_player_id(full)
                except Exception as e:
                    print(f"⚠️ {season}: NHL search error for '{full}': {e}")
                    skipped += 1
                    continue

            if pid is None:
                print(f"❌ {season}: no NHL match: '{full}' ({url})")
                skipped += 1
                continue

            # 3) Persist mapping if we have Spotrac id (only after NHL resolved)
            if spotrac_id is not None:
                upsert_spotrac_to_nhl(
                    conn,
                    map_table=map_table,
                    spotrac_id=int(spotrac_id),
                    player_id=int(pid),
                    confidence="high",
                    source="spotrac_url+nhl_search",
                )

            # 4) Upsert player_info
            upsert_dim_player_info(conn, int(pid), first, last)

            # 5) Update cap-hit row using unique natural key
            result = conn.execute(
                text(f"""
                    UPDATE {cap_table}
                    SET player_id = :pid
                    WHERE spotrac_url = :url
                      AND player_id IS NULL
                """),
                {"pid": int(pid), "url": url},
            )

            if result.rowcount and result.rowcount > 0:
                resolved += 1
            else:
                # likely already updated in a prior run
                skipped += 1

            time.sleep(0.08)

        print(f"✅ {season}: resolved={resolved}, skipped={skipped}")

    engine.dispose()


if __name__ == "__main__":
    for s in discover_cap_hit_seasons():
        backfill_season(s)
    # for s in [20152016]:
    #     backfill_season(s)
