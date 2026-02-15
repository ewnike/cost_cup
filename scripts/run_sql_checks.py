"""Docstring for scripts.run_sql_checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

from db_utils import get_db_engine

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import re

from db_utils import get_db_engine  # noqa: E402

_BLOCK_COMMENT_RE = re.compile(r"^\s*/\*.*?\*/\s*", re.DOTALL)


def parse_args() -> argparse.Namespace:
    """
    Docstring for parse_args.

    :return: Description
    :rtype: Namespace
    """
    p = argparse.ArgumentParser(description="Run SQL sanity checks from a directory.")
    p.add_argument(
        "--dir",
        default="sql/sanity",
        help="Directory containing .sql files (default: sql/sanity)",
    )
    return p.parse_args()


def _looks_like_select(sql: str) -> bool:
    """
    Heuristic: treat as "returns rows".

    If it starts with SELECT/WITH (or EXPLAIN SELECT/WITH),
    ignoring leading whitespace and comments.
    """
    s = sql.strip()

    # Strip any number of leading /* ... */ block comments
    while True:
        m = _BLOCK_COMMENT_RE.match(s)
        if not m:
            break
        s = s[m.end() :].lstrip()

    # Strip any number of leading -- line comments
    while s.startswith("--"):
        nl = s.find("\n")
        if nl == -1:
            return False
        s = s[nl + 1 :].lstrip()

        # after stripping a line comment, also strip block comments if they appear next
        while True:
            m = _BLOCK_COMMENT_RE.match(s)
            if not m:
                break
            s = s[m.end() :].lstrip()

    s_upper = s.upper()

    # Optional: treat EXPLAIN (SELECT|WITH) as row-returning
    if s_upper.startswith("EXPLAIN"):
        s_upper = s_upper[len("EXPLAIN") :].lstrip()

    return s_upper.startswith("SELECT") or s_upper.startswith("WITH")


def _format_cell(v) -> str:
    if v is None:
        return "NULL"
    # keep it readable for wide fields
    s = str(v)
    s = s.replace("\n", "\\n")
    return s


def _print_pretty_table(cols: list[str], rows: list[tuple], *, max_width: int = 48) -> None:
    """
    Print rows as a simple ASCII table.

    - max_width truncates very wide columns so your terminal stays sane.
    """
    # Compute column widths
    widths = [min(len(c), max_width) for c in cols]
    for r in rows:
        for i, v in enumerate(r[: len(cols)]):
            cell = _format_cell(v)
            widths[i] = min(max(widths[i], len(cell)), max_width)

    def clip(s: str, w: int) -> str:
        if len(s) <= w:
            return s
        if w <= 1:
            return s[:w]
        return s[: w - 1] + "…"

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    header = (
        "| " + " | ".join(clip(c, widths[i]).ljust(widths[i]) for i, c in enumerate(cols)) + " |"
    )

    print(sep)
    print(header)
    print(sep)
    for r in rows:
        line = (
            "| "
            + " | ".join(clip(_format_cell(v), widths[i]).ljust(widths[i]) for i, v in enumerate(r))
            + " |"
        )
        print(line)
    print(sep)


def main() -> None:
    """Run .sql sanity checks."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir",
        default="sql/sanity",
        help="Directory containing .sql checks (default: sql/sanity)",
    )
    parser.add_argument(
        "--print-results",
        action="store_true",
        help="Print result rows for SELECT/WITH queries that return rows.",
    )
    parser.add_argument(
        "--print-only",
        default="",
        help="Only print results for files whose name contains this substring (e.g. 'b_' or '05b').",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max rows to print per query (default: 50).",
    )
    args = parser.parse_args()

    sql_dir = Path(args.dir)
    engine = get_db_engine()
    files = sorted(sql_dir.glob("*.sql"))

    if not files:
        raise FileNotFoundError(f"No .sql files found in {sql_dir}")

    # ✅ Preflight: show exactly what will be run (and in what order)
    print(f"\nSQL_DIR: {sql_dir.resolve()}")
    print("Files to run (in order):")
    for i, f in enumerate(files, start=1):
        print(f"  {i:02d}. {f.name}")

    try:
        with engine.begin() as conn:
            for f in files:
                sql = f.read_text(encoding="utf-8").strip()
                if not sql:
                    continue

                print(f"\n=== RUN {f.name} ===")
                result = conn.execute(text(sql))

                # Print rows only if:
                #  - user asked for it
                #  - it's a SELECT/WITH
                #  - and file matches --print-only (if provided)
                should_print = args.print_results and _looks_like_select(sql)
                if args.print_only and args.print_only not in f.name:
                    should_print = False

                if should_print:
                    rows = result.fetchmany(args.limit)
                    if rows:
                        cols = list(result.keys())
                        rows_tuples = [tuple(r) for r in rows]
                        _print_pretty_table(cols, rows_tuples)
                        # show whether there are more rows not printed
                        extra = result.fetchone()
                        if extra is not None:
                            print(f"... (more rows not shown; limit={args.limit})")
                    else:
                        print("(no rows)")

                print(f"✅ OK {f.name}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
