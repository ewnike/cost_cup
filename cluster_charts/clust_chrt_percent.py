import argparse

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import text

from db_utils import get_db_engine


def fetch_counts(table: str) -> pd.DataFrame:
    sql = f"""
    select season, cluster, count(*) as n
    from {table}
    group by 1,2
    order by 1,2;
    """
    engine = get_db_engine()
    try:
        return pd.read_sql_query(text(sql), engine)
    finally:
        engine.dispose()


def plot_stacked(df: pd.DataFrame, title: str, as_pct: bool) -> None:
    pivot = df.pivot(index="season", columns="cluster", values="n").fillna(0).sort_index()

    if as_pct:
        pivot = pivot.div(pivot.sum(axis=1), axis=0) * 100
        ylabel = "Percent of player-seasons"
    else:
        ylabel = "Player-seasons"

    ax = pivot.plot(kind="bar", stacked=True, figsize=(10, 5))
    ax.set_title(title)
    ax.set_xlabel("Season")
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    plt.show()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pos", choices=["F", "D", "both"], default="F")
    ap.add_argument("--pct", action="store_true", help="Plot percentages instead of counts")
    args = ap.parse_args()

    targets = []
    if args.pos in ("F", "both"):
        targets.append(("F", "mart.player_season_clusters_modern_truth_f", "Forwards"))
    if args.pos in ("D", "both"):
        targets.append(("D", "mart.player_season_clusters_modern_truth_d", "Defense"))

    for _, table, label in targets:
        df = fetch_counts(table)
        plot_stacked(
            df,
            title=f"{label} cluster distribution by season ({'%' if args.pct else 'counts'})",
            as_pct=args.pct,
        )


if __name__ == "__main__":
    main()
