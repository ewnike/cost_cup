import json
import os

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    log_loss,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from db_utils import get_db_engine

SQL_SEASONS = """
SELECT DISTINCT season
FROM mart.v_player_season_archetypes_modern_regulars
ORDER BY season;
"""

SQL_TRAIN = """
WITH base AS (
  SELECT
    season,
    player_id,
    pos_group,
    cluster,
    toi_es_sec,
    toi_per_game,
    es_share,
    p60, s60,
    cf60, ca60,
    cf_percent,
    es_net60,
    fo_win_pct,
    hit60, blk60, take60, give60, penl60
  FROM mart.v_player_season_archetypes_modern_regulars
  WHERE pos_group = :pos_group
),
pairs AS (
  SELECT
    b.player_id,
    b.season AS season_t,
    b.cluster AS cluster_t,
    b2.cluster AS cluster_next,

    b.toi_es_sec,
    b.toi_per_game,
    b.es_share,
    b.p60, b.s60,
    b.cf60, b.ca60,
    b.cf_percent,
    b.es_net60,
    b.fo_win_pct,
    b.hit60, b.blk60, b.take60, b.give60, b.penl60
  FROM base b
  JOIN base b2
    ON b2.player_id = b.player_id
   AND b2.season = b.season + 10001
)
SELECT *
FROM pairs
ORDER BY season_t, player_id;
"""


FEATURE_COLS = [
    # current cluster as numeric feature
    "cluster_t",
    # usage / context
    "toi_es_sec",
    "toi_per_game",
    "es_share",
    # production + attempt rates
    "p60",
    "s60",
    "cf60",
    "ca60",
    "cf_percent",
    "es_net60",
    # style / misc
    "fo_win_pct",
    "hit60",
    "blk60",
    "take60",
    "give60",
    "penl60",
]


def season_next(season: int) -> int:
    return int(season) + 10001


def get_valid_test_seasons() -> list[int]:
    df = read_df(SQL_SEASONS, {})
    seasons = [int(s) for s in df["season"].tolist()]
    season_set = set(seasons)
    # test seasons are those season_t where season_t+1 exists
    return [s for s in seasons if season_next(s) in season_set]


def read_df(sql: str, params: dict) -> pd.DataFrame:
    engine = get_db_engine()
    try:
        return pd.read_sql_query(text(sql), engine, params=params)
    finally:
        engine.dispose()


def fit_one(pos_group: str, test_season: int = 20232024, out_dir: str = "model_out"):
    os.makedirs(out_dir, exist_ok=True)

    df = read_df(SQL_TRAIN, {"pos_group": pos_group})
    if df.empty:
        raise RuntimeError(f"No training rows for pos_group={pos_group}")

    # --- basic cleanup ---
    df = df.copy()
    df["cluster_next"] = df["cluster_next"].astype(int)
    df["cluster_t"] = df["cluster_t"].astype(int)

    # numeric conversions
    for c in FEATURE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=FEATURE_COLS + ["cluster_next", "season_t"]).reset_index(
        drop=True
    )
    df["season_t"] = df["season_t"].astype(int)

    # --- time split: train on seasons < test_season; test = test_season ---
    train_df = df[df["season_t"] < test_season].copy()
    test_df = df[df["season_t"] == test_season].copy()

    if test_df.empty:
        # fallback: last season as test
        test_season = int(df["season_t"].max())
        train_df = df[df["season_t"] < test_season].copy()
        test_df = df[df["season_t"] == test_season].copy()

    if train_df.empty or test_df.empty:
        print(f"SKIP pos={pos_group} test_season={test_season} (empty split)")
        return None, None

    min_train = 200 if pos_group == "F" else 80
    min_test = 50 if pos_group == "F" else 30

    if len(train_df) < min_train or len(test_df) < min_test:
        print(
            f"SKIP pos={pos_group} test_season={test_season} "
            f"(n_train={len(train_df)}, n_test={len(test_df)})"
        )
        return None, None

    X_train = train_df[FEATURE_COLS].to_numpy()
    y_train = train_df["cluster_next"].to_numpy()

    X_test = test_df[FEATURE_COLS].to_numpy()
    y_test = test_df["cluster_next"].to_numpy()

    # --- class balance ---
    train_counts = pd.Series(y_train).value_counts().sort_index().to_dict()
    test_counts = pd.Series(y_test).value_counts().sort_index().to_dict()

    # baseline: always predict most common class in train
    majority = int(pd.Series(y_train).value_counts().idxmax())
    y_base = np.full_like(y_test, fill_value=majority)
    baseline_acc = accuracy_score(y_test, y_base)

    # --- model ---
    # multinomial logistic regression
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    multi_class="multinomial",
                    solver="lbfgs",
                    max_iter=2000,
                    C=1.0,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    classes = model.named_steps["clf"].classes_
    proba_raw = model.predict_proba(X_test)
    pred_test = model.predict(X_test)

    # force proba into 3 columns in [0,1,2] order
    proba_test = np.zeros((proba_raw.shape[0], 3), dtype=float)
    for j, cls in enumerate(classes):
        proba_test[:, int(cls)] = proba_raw[:, j]

    acc = accuracy_score(y_test, pred_test)
    ll = log_loss(y_test, proba_test, labels=[0, 1, 2])
    cm = confusion_matrix(y_test, pred_test, labels=[0, 1, 2])

    report = classification_report(
        y_test, pred_test, labels=[0, 1, 2], output_dict=True
    )

    # --- save predictions for dashboard/DB ---
    out_pred = test_df[["player_id", "season_t", "cluster_t", "cluster_next"]].copy()
    out_pred["pos_group"] = pos_group
    out_pred["p_to0"] = proba_test[:, 0]
    out_pred["p_to1"] = proba_test[:, 1]
    out_pred["p_to2"] = proba_test[:, 2]

    # write CSV (safe / no DB write needed)
    out_csv = os.path.join(
        out_dir, f"transition_probs_{pos_group}_test_season_{test_season}.csv"
    )
    out_pred.to_csv(out_csv, index=False)

    # --- summary JSON ---
    summary = {
        "pos_group": pos_group,
        "test_season": test_season,
        "n_train": int(len(train_df)),
        "n_test": int(len(test_df)),
        "train_class_counts": train_counts,
        "test_class_counts": test_counts,
        "baseline_majority_class": majority,
        "baseline_accuracy": float(baseline_acc),
        "model_accuracy": float(acc),
        "log_loss": float(ll),
        "confusion_matrix_rows_true_cols_pred_0_1_2": cm.tolist(),
    }

    out_json = os.path.join(
        out_dir, f"transition_model_summary_{pos_group}_test_season_{test_season}.json"
    )
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))
    print("\nTop-level classification report:")
    print(pd.DataFrame(report).T[["precision", "recall", "f1-score", "support"]])

    print(f"\nSaved predictions: {out_csv}")
    print(f"Saved summary:     {out_json}")

    return out_pred, summary


if __name__ == "__main__":
    out_dir = "model_out"
    test_seasons = sorted(set(get_valid_test_seasons()))
    # test_seasons = get_valid_test_seasons()

    priority_ts = 20232024

    for pos in ["F", "D"]:
        print(f"\n--- Running pos={pos} test_season={priority_ts} ---")
        out_pred, summary = fit_one(pos, test_season=priority_ts, out_dir=out_dir)
        if out_pred is None:
            print(f"{pos} model skipped for test_season={priority_ts}.")

    for pos in ["F", "D"]:
        for ts in test_seasons:
            if ts == priority_ts:
                continue
            print(f"\n--- Running pos={pos} test_season={ts} ---")
            out_pred, summary = fit_one(pos, test_season=ts, out_dir=out_dir)
            if out_pred is None:
                print(f"{pos} model skipped for test_season={ts}.")
