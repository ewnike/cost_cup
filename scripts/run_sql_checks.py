"""
scripts.run_sql_checks.

Run SQL sanity/validation checks from a directory of .sql files.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from sqlalchemy import text

# Ensure repo root is on sys.path so "import db_utils" works when running:
#   python -m scripts.run_sql_checks ...
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_utils import get_db_engine  # noqa: E402


_BLOCK_COMMENT_RE = re.compile(r"^\s*/\*.*?\*/\s*", re.DOTALL)


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
    s = str(v).replace("\n", "\\n")
    return s


def _print_pretty_table(cols: list[str], rows: list[tuple], *, max_width: int = 48) -> None:
    """Print rows as a simple ASCII table."""
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
    parser = argparse.ArgumentParser(description="Run SQL sanity checks from a directory.")
    parser.add_argument("--dir", default="sql/sanity")
    parser.add_argument("--print-results", action="store_true")
    parser.add_argument("--print-only", default="")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--continue-on-error", action="store_true")
    args = parser.parse_args()

    sql_dir = Path(args.dir)
    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No .sql files found in {sql_dir}")

    engine = get_db_engine()
    failed_files: list[str] = []
    fail_count = 0

    try:
        for f in files:
            sql = f.read_text(encoding="utf-8").strip()
            if not sql:
                continue

            print(f"\n=== RUN {f.name} ===")

            try:
                with engine.connect() as conn:
                    with conn.begin():  # ✅ per-file transaction; auto rollback on error
                        result = conn.execute(text(sql))

                    should_print = args.print_results and _looks_like_select(sql)
                    if args.print_only and args.print_only not in f.name:
                        should_print = False

                    if should_print:
                        rows = result.fetchmany(args.limit)
                        if rows:
                            cols = list(result.keys())
                            _print_pretty_table(cols, [tuple(r) for r in rows])
                            extra = result.fetchone()
                            if extra is not None:
                                print(f"... (more rows not shown; limit={args.limit})")
                        else:
                            print("(no rows)")

                print(f"✅ OK {f.name}")

            except Exception as e:
                fail_count += 1
                failed_files.append(f.name)
                print(f"❌ FAIL {f.name}: {e}")
                if not args.continue_on_error:
                    raise

    finally:
        engine.dispose()  # ✅ only once, at the end

    if failed_files:
        print("\n====================")
        print(f"❌ Failures: {fail_count}")
        for n in failed_files:
            print(f" - {n}")
        raise SystemExit(1)

    print("\n====================")
    print("✅ All checks passed.")


if __name__ == "__main__":
    main()
