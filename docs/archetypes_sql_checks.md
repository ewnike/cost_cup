# Archetypes SQL Checks (Modern Pipeline)

**Project:** Cost of Cup  
**Last updated:** 2026-02-02  
**Purpose:** Reproducible SQL sanity checks for modern player-game → player-season features and clusters.

> Conventions used below:
> - Modern seasons are in: `20182019 ... 20242025`
> - Schemas:
>   - `raw` (raw RTSS pbp tables like `raw_pbp_{season}`)
>   - `derived` (views like `game_plays_{season}_from_raw_pbp`, `raw_shifts_resolved`)
>   - `mart` (final marts and feature tables)
> - Replace `:season` with an integer like `20192020` in your SQL editor, or hardcode it.

---

## 0) Quick: list tables by pattern

### List mart player-game tables
```sql
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'mart'
  AND table_name ILIKE 'player_game_%'
ORDER BY table_name;
```

### List raw pbp tables
```sql
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'raw'
  AND table_name ILIKE 'raw_pbp_%'
ORDER BY table_name;
```

## 1) Feature table coverage & season counts

### Total rows + min/max season modern features
```sql
SELECT
  COUNT(*) AS rows,
  MIN(season) AS min_season,
  MAX(season) AS max_season
FROM mart.player_season_archetype_features_modern;
```

### Per-season row counts(features)
```sql
SELECT
  season,
  COUNT(*) AS rows
FROM mart.player_season_archetype_features_modern
GROUP BY season
ORDER BY season;
```

### Total rows + min/max season cluster table
```sql
SELECT
  COUNT(*) AS rows,
  MIN(season) AS min_season,
  MAX(season) AS max_season
FROM mart.player_season_clusters_modern;
```

### Per-season row counts(clusters)
```sql
SELECT
  season,
  COUNT(*) AS rows
FROM mart.player_season_clusters_modern
GROUP BY season
ORDER BY season;
```

## 2) Null/Nan checks for key features
####     Note: In Postgres, NaN can appear only in floating types. Use x <> x to  detect NaN.

### Null counts by feature(features table)
```sql
SELECT
  COUNT(*) AS total_rows,
  SUM((g60 IS NULL)::int)        AS g60_null,
  SUM((a60 IS NULL)::int)        AS a60_null,
  SUM((p60 IS NULL)::int)        AS p60_null,
  SUM((s60 IS NULL)::int)        AS s60_null,
  SUM((hit60 IS NULL)::int)      AS hit60_null,
  SUM((blk60 IS NULL)::int)      AS blk60_null,
  SUM((take60 IS NULL)::int)     AS take60_null,
  SUM((give60 IS NULL)::int)     AS give60_null,
  SUM((penl60 IS NULL)::int)     AS penl60_null,
  SUM((fo_win_pct IS NULL)::int) AS fo_win_pct_null,
  SUM((toi_per_game IS NULL)::int) AS toi_per_game_null,
  SUM((es_share IS NULL)::int)     AS es_share_null,
  SUM((cf60 IS NULL)::int)         AS cf60_null,
  SUM((ca60 IS NULL)::int)         AS ca60_null,
  SUM((cf_percent IS NULL)::int)   AS cf_percent_null
FROM mart.player_season_archetype_features_modern;
```

### NaN counts (if any float columns can contain NaN)
```sql
SELECT
  SUM((cf60 <> cf60)::int)       AS cf60_nan,
  SUM((ca60 <> ca60)::int)       AS ca60_nan,
  SUM((cf_percent <> cf_percent)::int) AS cf_percent_nan,
  SUM((fo_win_pct <> fo_win_pct)::int) AS fo_win_pct_nan
FROM mart.player_season_archetype_features_modern;
```

## 3) Cluster composition & summaries

### Cluster sizes
```sql
SELECT
  cluster,
  COUNT(*) AS n
FROM mart.player_season_clusters_modern
GROUP BY cluster
ORDER BY cluster;
```

### Cluster summary means (attach features)
```sql
SELECT
  c.cluster,
  COUNT(*) AS n,
  AVG(f.p60) AS avg_p60,
  AVG(f.s60) AS avg_s60,
  AVG(f.hit60) AS avg_hit60,
  AVG(f.blk60) AS avg_blk60,
  AVG(f.fo_win_pct) AS avg_fo,
  AVG(f.cf_percent) AS avg_cf_pct,
  AVG(f.toi_per_game) AS avg_toi_pg
FROM mart.player_season_clusters_modern c
JOIN mart.player_season_archetype_features_modern f
  ON f.season = c.season
 AND f.player_id = c.player_id
 AND f.team_id = c.team_id
GROUP BY c.cluster
ORDER BY c.cluster;
```

## 4) Position distribution inside clusters
#### Assumes `dim.player_info` has a position field. If it is named differently, change `primaryPosition`. 
#### If you store position elsewhere, replace the join accordingly.

### Counts by cluster and positiion (F/D only example)
```sql
WITH pos AS (
  SELECT
    player_id::bigint AS player_id,
    CASE
      WHEN primaryPosition IN ('D') THEN 'D'
      WHEN primaryPosition IN ('C','L','R','F') THEN 'F'
      ELSE NULL
    END AS pos
  FROM dim.player_info
)
SELECT
  c.cluster,
  p.pos,
  COUNT(*) AS n
FROM mart.player_season_clusters_modern c
JOIN pos p
  ON p.player_id = c.player_id
WHERE p.pos IN ('F','D')
GROUP BY c.cluster, p.pos
ORDER BY c.cluster, p.pos;
```

### Percent within each cluster
```sql
WITH pos AS (
  SELECT
    player_id::bigint AS player_id,
    CASE
      WHEN primaryPosition IN ('D') THEN 'D'
      WHEN primaryPosition IN ('C','L','R','F') THEN 'F'
      ELSE NULL
    END AS pos
  FROM dim.player_info
),
counts AS (
  SELECT
    c.cluster,
    p.pos,
    COUNT(*)::numeric AS n
  FROM mart.player_season_clusters_modern c
  JOIN pos p
    ON p.player_id = c.player_id
  WHERE p.pos IN ('F','D')
  GROUP BY c.cluster, p.pos
),
tot AS (
  SELECT cluster, SUM(n) AS n_total
  FROM counts
  GROUP BY cluster
)
SELECT
  counts.cluster,
  counts.pos,
  counts.n::int AS n,
  ROUND(100.0 * counts.n / tot.n_total, 1) AS pct_in_cluster
FROM counts
JOIN tot USING (cluster)
ORDER BY counts.cluster, counts.pos;
```

## 5) Player cluster stability/transitions

### A) How many different clusters do each player occupy?
```sql
SELECT
  player_id,
  COUNT(DISTINCT cluster) AS n_clusters,
  COUNT(DISTINCT season) AS seasons
FROM mart.player_season_clusters_modern
GROUP BY player_id
HAVING COUNT(DISTINCT season) >= 3
ORDER BY n_clusters DESC, seasons DESC, player_id
LIMIT 50;
```

### B) Most common cluster per player + share
```sql
WITH x AS (
  SELECT
    player_id,
    COUNT(DISTINCT season) AS seasons
  FROM mart.player_season_clusters_modern
  GROUP BY player_id
),
freq AS (
  SELECT
    c.player_id,
    c.cluster,
    COUNT(*) AS n_in_cluster
  FROM mart.player_season_clusters_modern c
  GROUP BY c.player_id, c.cluster
),
ranked AS (
  SELECT
    f.player_id,
    x.seasons,
    f.cluster,
    f.n_in_cluster,
    ROW_NUMBER() OVER (PARTITION BY f.player_id ORDER BY f.n_in_cluster DESC, f.cluster) AS rn
  FROM freq f
  JOIN x ON x.player_id = f.player_id
)
SELECT
  player_id,
  seasons,
  cluster AS most_common_cluster,
  n_in_cluster AS seasons_in_most_common,
  ROUND(n_in_cluster::numeric / seasons, 3) AS share_most_common
FROM ranked
WHERE rn = 1
ORDER BY share_most_common ASC, seasons DESC
LIMIT 50;
```

### C)Transition rate across seasons (player-level)
#### Transition rate = transitions/(seasons-1)
#### This assumes the cluster table has one row per season(or at least one).
```sql
WITH seq AS (
  SELECT
    player_id,
    season,
    cluster,
    LAG(cluster) OVER (PARTITION BY player_id ORDER BY season) AS prev_cluster
  FROM mart.player_season_clusters_modern
),
agg AS (
  SELECT
    player_id,
    COUNT(*) AS seasons,
    SUM((prev_cluster IS NOT NULL)::int) AS transitions_possible,
    SUM((prev_cluster IS NOT NULL AND cluster <> prev_cluster)::int) AS transitions
  FROM seq
  GROUP BY player_id
)
SELECT
  player_id,
  seasons,
  transitions_possible,
  transitions,
  CASE WHEN transitions_possible > 0
       THEN ROUND(transitions::numeric / transitions_possible, 3)
       ELSE NULL
  END AS transition_rate
FROM agg
WHERE seasons >= 6
ORDER BY transition_rate DESC, seasons DESC
LIMIT 50;
```

## 6) Player-game table rowcount checks (per season)

### A) player_game_stats rows per season
```sql
-- Replace 20192020 with the season you want
SELECT COUNT(*) AS stats_rows
FROM mart.player_game_stats_20192020;
```

### B) player_game_boxscore rows per season
```sql
SELECT COUNT(*) AS box_rows
FROM mart.player_game_boxscore_20192020;
```

### C) Join match rate between player_game_stats and player_game_boxscore
### same season
```sql
WITH s AS (
  SELECT season, game_id, player_id, team_id
  FROM mart.player_game_stats_20192020
),
b AS (
  SELECT season, game_id, player_id, team_id
  FROM mart.player_game_boxscore_20192020
)
SELECT
  (SELECT COUNT(*) FROM s) AS stats_rows,
  (SELECT COUNT(*) FROM b) AS box_rows,
  (SELECT COUNT(*) FROM s JOIN b USING (season, game_id, player_id, team_id)) AS matched_rows;
```

## 7) Raw PBP event sanity checks

### A) Event type frequency (single season)
```sql
SELECT
  event_type,
  COUNT(*) AS n
FROM raw.raw_pbp_20182019
WHERE season = 20182019
  AND session = 'R'
GROUP BY event_type
ORDER BY n DESC;
```

### B) Any event types outside expected list?
```sql
SELECT DISTINCT event_type
FROM raw.raw_pbp_20182019
WHERE season = 20182019
  AND session = 'R'
  AND event_type NOT IN ('GOAL','SHOT','MISS','BLOCK','HIT','GIVE','TAKE','FAC','PENL')
ORDER BY event_type;
```

### C) Player-role presence checks(who appears in p1/p2/p3 for key events)
```sql
WITH base AS (
  SELECT event_type, event_player_1, event_player_2, event_player_3
  FROM raw.raw_pbp_20182019
  WHERE season = 20182019
    AND session = 'R'
    AND event_type IN ('GOAL','SHOT','HIT')
),
unpivot AS (
  SELECT event_type, 'p1' AS role, event_player_1 AS player_raw FROM base WHERE event_player_1 IS NOT NULL
  UNION ALL
  SELECT event_type, 'p2' AS role, event_player_2 AS player_raw FROM base WHERE event_player_2 IS NOT NULL
  UNION ALL
  SELECT event_type, 'p3' AS role, event_player_3 AS player_raw FROM base WHERE event_player_3 IS NOT NULL
)
SELECT
  event_type,
  role,
  COUNT(*) AS n
FROM unpivot
GROUP BY event_type, role
ORDER BY event_type, role;
```

## 8) Index sanity (optional)

### Check index exists on a atable
```sql
SELECT
  schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'mart'
  AND tablename = 'toi_total_20192020'
ORDER BY indexname;
```

## Notes/Known Issues:

####  Faceoff mapping assumptions (FAC):
   * p1 winner, p2 loser (assumption based on RTSS conventions)
   * Should be validated against at least one known boxscore source.

####  Penalty mapping (PENL):
   * p1 penalized assumed; p2 often “drawn by”
   * PIM minutes are not computed yet (would require parsing penalty detail strings).

####  `fo_win_pct nulls`:
   * Decide imputation strategy and document it.

## Reproducibility

All sanity-check SQL used to validate counts, nulls, joins, and transitions is stored here:

- `docs/archetypes_sql_checks.md`

Key checks I ran most often:
- Feature table coverage (row count + min/max season)
- Cluster size + per-cluster means
- Cluster × position distribution
- player_game_stats ↔ boxscore join match rate















