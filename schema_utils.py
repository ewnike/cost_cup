"""
Small helpers for building schema-qualified database identifiers.

Why this exists:
- Your database uses multiple schemas (raw, dim, derived, mart).
- Relying on Postgres search_path can cause subtle bugs when teammates run queries
  or when you deploy to AWS.
- These helpers centralize schema naming so code stays consistent and “teammate-proof”.

Usage:
- SCHEMA is defined in constants.py, e.g.:
    SCHEMA = {"raw": "raw", "dim": "dim", "derived": "derived", "mart": "mart"}

- Build fully-qualified identifiers:
    fq("raw", "game")                        -> "raw.game"
    fq("derived", f"game_plays_{season}_from_raw_pbp")
                                           -> "derived.game_plays_20192020_from_raw_pbp"

Notes:
- This only formats names (strings). It does not validate that objects exist.
- If you need safe quoting for mixed-case / special characters, add a separate
  quoting helper (or use SQLAlchemy's identifier quoting).

"""

from __future__ import annotations

from .constants import SCHEMA


def qident(name: str) -> str:
    """
    Quote an identifier safely for Postgres.

    - Adds double-quotes and escapes embedded quotes.
    - Lets you keep names like: timeOnIce, CF_Percent, etc.
    """
    return '"' + name.replace('"', '""') + '"'


def fq(schema_key: str, obj_name: str, *, quote: bool = False) -> str:
    """
    Fully qualify an identifier like schema.table_or_view.

    If quote=True, returns: "schema"."object"
    """
    schema = SCHEMA[schema_key]
    if quote:
        return f"{qident(schema)}.{qident(obj_name)}"
    return f"{schema}.{obj_name}"


def fqs(schema_key: str, obj_name: str) -> str:
    """
    Quote fully-qualified identifier.

    Recommended when you embed in raw SQL strings.
    """
    return fq(schema_key, obj_name, quote=True)
