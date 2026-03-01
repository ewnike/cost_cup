#!/usr/bin/env python3
"""
scrapers/pipeline_scraper.py

Orchestrate scraping + downstream merge/insert for Spotrac-ish datasets.

Usage examples (from repo root):
  python -m scrapers.pipeline_scraper --help
  python -m scrapers.pipeline_scraper --steps caphit payroll team_record
  python -m scrapers.pipeline_scraper --steps caphit etl_merge --season 20242025
  python -m scrapers.pipeline_scraper --all --season 20242025

Notes:
- Preferred: import + call functions from scrapers/*.py and scrapers/etl/*.py
- Fallback: run modules via subprocess if a step is still a "script-style" file.

"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from typing import Callable

# --- OPTIONAL: if you have centralized logging, import it here ---
# from log_utils import get_logger
# log = get_logger(__name__)


@dataclass(frozen=True)
class Step:
    name: str
    runner: Callable[[argparse.Namespace], None]
    help: str


def _run_module(module: str, args: list[str]) -> None:
    """Fallback: run `python -m module ...`."""
    cmd = [sys.executable, "-m", module, *args]
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)


# -----------------------------
# Step runners (edit these)
# -----------------------------


def step_caphit(ns: argparse.Namespace) -> None:
    """
    Preferred:
      from scrapers.caphit import scrape_player_caphit
      scrape_player_caphit(season=ns.season, out_dir=ns.out_dir, headless=ns.headless)

    Fallback for now:
      run the module if caphit.py still uses if __name__ == '__main__' CLI parsing
    """
    # preferred (uncomment when caphit.py exposes a function)
    # from scrapers.caphit import run as run_caphit
    # run_caphit(season=ns.season, out_dir=ns.out_dir, headless=ns.headless)

    # fallback:
    args = []
    if ns.season is not None:
        args += ["--season", str(ns.season)]
    if ns.out_dir:
        args += ["--out-dir", ns.out_dir]
    if ns.headless:
        args += ["--headless"]
    _run_module("scrapers.caphit", args)


def step_payroll(ns: argparse.Namespace) -> None:
    # preferred:
    # from scrapers.payroll import run as run_payroll
    # run_payroll(season=ns.season, out_dir=ns.out_dir, headless=ns.headless)

    # fallback:
    args = []
    if ns.season is not None:
        args += ["--season", str(ns.season)]
    if ns.out_dir:
        args += ["--out-dir", ns.out_dir]
    if ns.headless:
        args += ["--headless"]
    _run_module("scrapers.payroll", args)


def step_team_record(ns: argparse.Namespace) -> None:
    # preferred:
    # from scrapers.team_record import run as run_team_record
    # run_team_record(season=ns.season, out_dir=ns.out_dir, headless=ns.headless)

    # fallback:
    args = []
    if ns.season is not None:
        args += ["--season", str(ns.season)]
    if ns.out_dir:
        args += ["--out-dir", ns.out_dir]
    if ns.headless:
        args += ["--headless"]
    _run_module("scrapers.team_record", args)


def step_etl_merge(ns: argparse.Namespace) -> None:
    """
    This is where you stitch your scraped CSVs into your DB tables.

    You currently have:
      scrapers/etl/scraped_team_data_merge_and_insert.py
      scrapers/etl/scraped_team_data_later_seasons_merge_and_insert.py

    Decide whether you want one or both. For now we support one "etl_merge" step that can call both.
    """
    args = []
    if ns.season is not None:
        args += ["--season", str(ns.season)]
    if ns.dry_run:
        args += ["--dry-run"]

    # fallback run both, or pick based on season boundary
    _run_module("scrapers.etl.scraped_team_data_merge_and_insert", args)
    _run_module("scrapers.etl.scraped_team_data_later_seasons_merge_and_insert", args)


STEPS: dict[str, Step] = {
    "caphit": Step("caphit", step_caphit, "Scrape player cap hits"),
    "payroll": Step("payroll", step_payroll, "Scrape team payroll"),
    "team_record": Step(
        "team_record", step_team_record, "Scrape team records/standings"
    ),
    "etl_merge": Step(
        "etl_merge", step_etl_merge, "Merge/insert scraped outputs into DB"
    ),
}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run scrapers pipeline.")
    p.add_argument(
        "--steps",
        nargs="+",
        choices=list(STEPS.keys()),
        help=f"Steps to run. Options: {', '.join(STEPS.keys())}",
    )
    p.add_argument("--all", action="store_true", help="Run all steps in default order.")
    p.add_argument(
        "--season", type=int, default=None, help="Season (e.g., 20242025). Optional."
    )
    p.add_argument(
        "--out-dir", type=str, default="", help="Output directory for scraped files."
    )
    p.add_argument(
        "--headless", action="store_true", help="Run browser headless (if supported)."
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Do not write to DB (if supported)."
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(argv or sys.argv[1:])

    if not ns.all and not ns.steps:
        print("Pick --all or --steps ...")
        return 2

    run_order: list[str] = ["caphit", "payroll", "team_record", "etl_merge"]
    selected = run_order if ns.all else list(ns.steps)

    for name in selected:
        step = STEPS[name]
        print(f"\n=== STEP: {step.name} ===")
        step.runner(ns)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
