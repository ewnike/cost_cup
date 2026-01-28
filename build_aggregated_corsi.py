"""
Build_aggregated_corsi.py.

Aggregates mart.player_game_es_{season} to mart.aggregated_corsi_{season}
and enriches with dim.player_info + dim.player_cap_hit_{season}.
"""

from __future__ import annotations

import pandas as pd

from .constants import SCHEMA, SEASONS_ALL  # include 20152016..20242025
from .db_utils import get_db_engine
from .schema_utils import fq


def main() -> None:
    """Build aggregated corsi."""
    engine = get_db_engine()

    for season in SEASONS_ALL:
        pges = fq("mart", f"player_game_es_{season}")
        pi = fq("dim", "player_info")
        cap = fq("dim", f"player_cap_hit_{season}")

        df = pd.read_sql(f"SELECT * FROM {pges}", engine)
        if df.empty:
            print(f"⚠️ {season}: missing/empty {pges}")
            continue

        # totals by team (for primary team)
        by_team = df.groupby(["player_id", "team_id"], as_index=False).agg(
            cf_total=("cf", "sum"),
            ca_total=("ca", "sum"),
            toi_total=("toi_sec", "sum"),
            game_count=("game_id", "nunique"),
        )

        # primary team = max toi_total
        by_team["rank"] = by_team.groupby("player_id")["toi_total"].rank(
            method="first", ascending=False
        )
        primary = by_team[by_team["rank"] == 1][["player_id", "team_id"]].rename(
            columns={"team_id": "team_id_primary"}
        )

        totals = by_team.groupby("player_id", as_index=False).agg(
            cf_total=("cf_total", "sum"),
            ca_total=("ca_total", "sum"),
            toi_sec=("toi_total", "sum"),
            game_count=("game_count", "sum"),
            multi_team=("team_id", "nunique"),
        )
        totals["multi_team"] = totals["multi_team"] > 1
        totals["corsi_total"] = totals["cf_total"] - totals["ca_total"]
        totals["cf_percent"] = (
            100.0 * totals["cf_total"] / (totals["cf_total"] + totals["ca_total"])
        ).fillna(0.0)
        totals["cf60"] = totals["cf_total"] * 3600.0 / totals["toi_sec"]
        totals["ca60"] = totals["ca_total"] * 3600.0 / totals["toi_sec"]

        out = totals.merge(primary, on="player_id", how="left")

        # enrich names + cap hits by player_id
        df_pi = pd.read_sql(f'SELECT player_id, "firstName", "lastName" FROM {pi}', engine)
        df_cap = pd.read_sql(f'SELECT player_id, "capHit", spotrac_url FROM {cap}', engine)

        out = out.merge(df_pi, on="player_id", how="left").merge(df_cap, on="player_id", how="left")
        out = out.rename(columns={"team_id_primary": "team_id"})

        table = f"aggregated_corsi_{season}"
        out.to_sql(table, engine, schema=SCHEMA["mart"], if_exists="replace", index=False)
        print(f"✅ {season}: wrote {SCHEMA['mart']}.{table} rows={len(out)}")

    engine.dispose()


if __name__ == "__main__":
    main()
