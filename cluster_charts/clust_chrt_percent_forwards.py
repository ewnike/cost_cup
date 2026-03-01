import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import text

from db_utils import get_db_engine

SQL = """
select season, cluster, count(*) as n
from mart.player_season_clusters_modern_truth_f
group by 1,2
order by 1,2;
"""

engine = get_db_engine()
df = pd.read_sql_query(text(SQL), engine)
engine.dispose()

pivot = df.pivot(index="season", columns="cluster", values="n").fillna(0).sort_index()
pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

ax = pivot_pct.plot(kind="bar", stacked=True, figsize=(10, 5))
ax.set_title("Forwards cluster distribution by season (%)")
ax.set_xlabel("Season")
ax.set_ylabel("Percent of player-seasons")
plt.tight_layout()
plt.show()
