#!/usr/bin/env python3
"""
all_seasons_model_probs.py

Train a supervised cluster transition model (multinomial logistic regression)
for F and D, per season split, and write probability outputs to CSV.

Run from repo root:
  python scripts/all_seasons_model_probs.py --dsn "$PSQL_DSN" --out-dir model_out

Notes:
- Uses mart.v_player_season_archetypes_modern_regulars as the source of features/cluster labels.
- Builds (season_t -> season_t+1) training rows using season arithmetic: season_next = season_t + 10001
- Trains separate models for F and D.

"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    log_loss,
)
from sqlalchemy import text

from db_utils import get_db_engine

# ----------------------------- SQL -----------------------------

SQL_SEASONS = """
SELECT DISTINCT season
FROM mart.v_player_season_archetypes_modern_regulars
ORDER BY season;
"""

SQL_BASE = """
SELECT
  season,
  player_id,
  team_id,
  team_code,
  pos_group,
  cluster,
  toi_es_sec,
  toi_per_game,
  es_share,
  p60,
  s60,
  cf60,
  ca60,
  cf_percent,
  es_net60
FROM mart.v_player_season_archetypes_modern_regulars
WHERE pos_group = :pos_group
  AND season BETWEEN :season_min AND :season_max
ORDER BY season, player_id;
"""


# -------------------------- structures -------------------------


@dataclass
class RunSummary:
    pos_group: str
    test_season: int
    n_train: int
    n_test: int
    train_class_counts: Dict[int, int]
    test_class_counts: Dict[int, int]
    baseline_majority_class: int
    baseline_accuracy: float
    model_accuracy: float
    log_loss: float
    confusion_matrix_rows_true_cols_pred_0_1_2: List[List[int]]


# ----------------------------- helpers -----------------------------


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_db_engine()
    try:
        return pd.read_sql_query(text(sql), engine, params=params or {})
    finally:
        engine.dispose()


def season_next(season: int) -> int:
    """20182019 -> 20192020 etc."""
    return int(season) + 10001


def build_pairs(df_base: pd.DataFrame) -> pd.DataFrame:
    """
    Input: df_base has one row per player-season with features + cluster.
    Output: one row per (player_id, season_t) with target cluster_next.
    """
    df = df_base.copy()

    # --- guardrails: remove duplicated column names + remove leftover season_next ---
    df = df.loc[:, ~df.columns.duplicated()]  # drops duplicate column labels
    df = df.drop(columns=["season_next"], errors="ignore")

    # season t rows (keep all features)
    df_t = df.copy()
    df_t["season_next"] = df_t["season"].map(season_next)

    # season t+1 rows (only need player_id, season, cluster)
    df_n = df[["player_id", "season", "cluster"]].copy()
    df_n = df_n.rename(columns={"season": "season_next", "cluster": "cluster_next"})

    # ensure df_n is unique on join keys (one target per player per next season)
    df_n = df_n.drop_duplicates(subset=["player_id", "season_next"], keep="last")

    assert df_t.columns.is_unique
    assert df_n.columns.is_unique

    # join on player_id and season_next
    pairs = df_t.merge(
        df_n,
        on=["player_id", "season_next"],
        how="inner",
        validate="many_to_one",
    )

    # rename t columns for clarity
    pairs = pairs.rename(columns={"season": "season_t", "cluster": "cluster_t"})

    return pairs


def make_feature_matrix(df_pairs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Define features X and target y.
    Keep it simple and numeric.
    """
    feature_cols = [
        "toi_per_game",
        "es_share",
        "p60",
        "s60",
        "cf60",
        "ca60",
        "cf_percent",
        "es_net60",
        # optional: include current cluster as a feature
        "cluster_t",
    ]
    X = df_pairs[feature_cols].copy()
    y = df_pairs["cluster_next"].astype(int)
    # ensure numeric
    for c in feature_cols:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    X = X.fillna(0.0)
    return X, y


def fit_and_score_multinom(
    df_pairs: pd.DataFrame,
    test_season: int,
    pos_group: str,
    out_dir: Path,
) -> RunSummary:
    """
    Train on all seasons_t < test_season, test on seasons_t == test_season.
    Write per-row prediction CSV for the test split.
    """
    df_train = df_pairs[df_pairs["season_t"] < test_season].copy()
    df_test = df_pairs[df_pairs["season_t"] == test_season].copy()

    # basic guard
    if df_train.empty or df_test.empty:
        # write an empty placeholder and return tiny summary
        out_csv = (
            out_dir / f"transition_probs_{pos_group}_test_season_{test_season}.csv"
        )
        df_test.to_csv(out_csv, index=False)

        return RunSummary(
            pos_group=pos_group,
            test_season=test_season,
            n_train=len(df_train),
            n_test=len(df_test),
            train_class_counts={},
            test_class_counts={},
            baseline_majority_class=-1,
            baseline_accuracy=0.0,
            model_accuracy=0.0,
            log_loss=float("nan"),
            confusion_matrix_rows_true_cols_pred_0_1_2=[
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
            ],
        )

    X_train, y_train = make_feature_matrix(df_train)
    X_test, y_test = make_feature_matrix(df_test)

    # multinomial logistic regression
    model = LogisticRegression(
        multi_class="multinomial",
        solver="lbfgs",
        max_iter=2000,
        n_jobs=None,
    )
    model.fit(X_train, y_train)

    # probs and preds
    proba = model.predict_proba(X_test)  # columns correspond to model.classes_
    preds = model.predict(X_test)

    # map probabilities to p_to0/p_to1/p_to2
    class_order = list(model.classes_)
    # create full 0/1/2 columns even if a class is missing in train
    p = {0: np.zeros(len(X_test)), 1: np.zeros(len(X_test)), 2: np.zeros(len(X_test))}
    for j, cls in enumerate(class_order):
        p[int(cls)] = proba[:, j]

    df_out = df_test[["player_id", "season_t", "cluster_t", "cluster_next"]].copy()
    df_out["pos_group"] = pos_group
    df_out["p_to0"] = p[0]
    df_out["p_to1"] = p[1]
    df_out["p_to2"] = p[2]

    out_csv = out_dir / f"transition_probs_{pos_group}_test_season_{test_season}.csv"
    df_out.to_csv(out_csv, index=False)

    # metrics
    train_counts = y_train.value_counts().to_dict()
    test_counts = y_test.value_counts().to_dict()
    majority_class = int(max(train_counts, key=train_counts.get))
    baseline_acc = float((y_test == majority_class).mean())
    model_acc = float(accuracy_score(y_test, preds))

    # log_loss needs labels list to be stable 0/1/2
    ll = float(log_loss(y_test, np.c_[p[0], p[1], p[2]], labels=[0, 1, 2]))

    cm = confusion_matrix(y_test, preds, labels=[0, 1, 2]).tolist()

    summary = RunSummary(
        pos_group=pos_group,
        test_season=test_season,
        n_train=len(df_train),
        n_test=len(df_test),
        train_class_counts={int(k): int(v) for k, v in train_counts.items()},
        test_class_counts={int(k): int(v) for k, v in test_counts.items()},
        baseline_majority_class=majority_class,
        baseline_accuracy=baseline_acc,
        model_accuracy=model_acc,
        log_loss=ll,
        confusion_matrix_rows_true_cols_pred_0_1_2=cm,
    )

    out_json = (
        out_dir / f"transition_model_summary_{pos_group}_test_season_{test_season}.json"
    )
    out_json.write_text(json.dumps(asdict(summary), indent=2))

    # optional: console report
    print("\n=== SUMMARY ===")
    print(json.dumps(asdict(summary), indent=2))
    print("\nTop-level classification report:")
    print(classification_report(y_test, preds, labels=[0, 1, 2], zero_division=0))

    print(f"\nSaved predictions: {out_csv}")
    print(f"Saved summary:     {out_json}")

    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--dsn",
        default=None,
        help="Optional. If your db_utils reads .env, you can omit.",
    )
    p.add_argument(
        "--out-dir", default="model_out", help="Output directory for CSV/JSON."
    )
    p.add_argument(
        "--season-min",
        type=int,
        default=20182019,
        help="Minimum season to include as season_t.",
    )
    p.add_argument(
        "--season-max",
        type=int,
        default=20242025,
        help="Maximum season to include in base extraction (includes season_t+1 for joins).",
    )
    p.add_argument(
        "--test-season",
        type=int,
        default=None,
        help="If set, run only this test season. Otherwise run all possible test seasons.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Seasons list from DB (ground truth list)
    df_seasons = read_df(SQL_SEASONS)
    seasons = [int(s) for s in df_seasons["season"].tolist()]

    # restrict to user bounds
    seasons = [s for s in seasons if args.season_min <= s <= args.season_max]
    if len(seasons) < 2:
        raise SystemExit("Not enough seasons available to build transitions.")

    # test seasons are season_t values that have season_t+1 also present
    valid_test_seasons = [
        s
        for s in seasons
        if season_next(s) in seasons and len([t for t in seasons if t < s]) > 0
    ]
    if args.test_season is not None:
        valid_test_seasons = [int(args.test_season)]

    for pos_group in ["F", "D"]:
        print(f"\n\n=== Building base for pos_group={pos_group} ===")
        df_base = read_df(
            SQL_BASE,
            params={
                "pos_group": pos_group,
                "season_min": min(seasons),
                "season_max": max(seasons),
            },
        )
        dupes = df_base.columns[df_base.columns.duplicated()].tolist()
        print("DUPLICATE COL NAMES:", dupes)
        print("ALL COLS:", df_base.columns.tolist())

        df_pairs = build_pairs(df_base)

        print(f"Base rows:  {len(df_base):,}")
        print(
            f"Pair rows:  {len(df_pairs):,}  (player-season_t with next season target)"
        )

        for test_season in valid_test_seasons:
            fit_and_score_multinom(
                df_pairs=df_pairs,
                test_season=test_season,
                pos_group=pos_group,
                out_dir=out_dir,
            )


if __name__ == "__main__":
    main()
