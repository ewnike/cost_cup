# Player Archetypes Summary (Modern Pipeline)

**Project:** Cost of Cup  
**Last updated:** 2026-02-02  
**Scope:** Modern seasons (20182019–20242025) in AWS-hosted PostgreSQL

---

## Table of Contents
1. [Purpose](#purpose)
2. [What We Built This Week](#what-we-built-this-week)
3. [Data Sources](#data-sources)
4. [Derived Tables Produced](#derived-tables-produced)
5. [Archetype Feature Engineering](#archetype-feature-engineering)
6. [Clustering Results](#clustering-results)
7. [Sanity Checks & Coverage](#sanity-checks--coverage)
8. [Leakage Risks & Confounding Variables](#leakage-risks--confounding-variables)
9. [How We’ll Use Archetypes Moving Forward](#how-well-use-archetypes-moving-forward)
10. [Next Steps](#next-steps)

---

## Purpose

We created **role-based player archetypes** for modern NHL seasons using AWS-hosted data derived from:
- **Play-by-play (PBP)** events (raw RTSS-style event stream)
- **Resolved shift data** (player IDs + time-on-ice)

The archetypes are intended to:
- Provide an interpretable, compact representation of player roles
- Enable **role transition analysis** (Markov framing)
- Feed **Bayesian / probabilistic matchup models** for predicting game outcomes
- Keep **Corsi metrics** central to the predictive story

---

## What We Built This Week

### Outcomes achieved
1) A consistent **modern player-game stats** pipeline that produces:
   - Even-strength possession (CF/CA) and rates (CF60/CA60/CF%)
   - Total and even-strength TOI

2) A **modern boxscore pipeline** derived from PBP events:
   - goals, assists, shots, hits, blocks, takeaways/giveaways, faceoffs, penalties taken

3) A **player-season feature table** spanning all modern seasons

4) A **player-season clustering model** (k=3) that outputs:
   - cluster assignments per (season, player_id, team_id)
   - interpretable cluster summaries (means, composition)
   - early transition analysis across seasons

---

## Data Sources

### Seasons
**SEASONS_MODERN**
- 20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025

### Core AWS Postgres inputs
- `raw.raw_pbp_{season}`  
  RTSS-like event stream. Key columns observed:
  `event_type`, `event_team`, `event_player_1/2/3`, `home_team`, `away_team`,
  plus game/time identifiers.

- `derived.game_plays_{season}_from_raw_pbp`  
  Normalized game_plays-like view including `time`, `team_id_for`, `team_id_against`, etc.

- `Canonical: raw.raw_shifts_resolved (and derived.raw_shifts_resolved is an alias)`
  Shift data with resolved player IDs and team codes, including:
  `game_id`, `player_id_resolved`, `team`, `position`, `seconds_start`, `seconds_end`, `season`, `session`.

- Dimensions:
  - `dim.dim_team_code` (team_code → team_id)
  - `dim.player_info` (player_id, first/last, and position metadata)

---

## Derived Tables Produced

### A) Player-game possession + TOI (even strength)
- `mart.player_game_stats_{season}`
- Columns (typical):
  - `season`
  - `game_id`
  - `player_id`
  - `team_id`
  - `cf`, `ca`
  - `toi_total_sec`, `toi_es_sec`
  - `cf60`, `ca60`, `cf_percent`

**Notes**
- Even-strength definition:
  - **ALLOW:** equal skater counts (5v5, 4v4, 3v3 OT, etc.)
  - **EXCLUDE:** imbalanced skater counts up to 6 (6v5, 5v4, 4v3, etc.)
  - Rule: `exclude = (team_1 != team_2) AND team_1 <= 6 AND team_2 <= 6`

### B) Player-game boxscore derived from PBP
- `mart.player_game_boxscore_{season}`
- Columns (typical):
  - `season, game_id, player_id, team_id`
  - `goals, assists, shots, hits, blocked`
  - `takeaways, giveaways`
  - `faceoff_wins, faceoff_taken`
  - `penalties_taken`
  - `points`

**Event type mapping used**
- `GOAL` (p1=goal, p2/p3=assist)
- `SHOT` (+ `GOAL` treated as a shot attempt for shooter)
- `HIT` (p1 credited)
- `BLOCK` (p1 credited)
- `TAKE` (p1 credited)
- `GIVE` (p1 credited)
- `FAC` (p1=winner, p2=loser assumed)
- `PENL` (p1 penalized assumed; p2 often “drawn-by”)

> We should document any deviations once validated against a known boxscore source.

### C) Player-season archetype features
- `mart.player_season_archetype_features_modern`
- One row per (season, player_id, team_id), aggregated from player-game stats.

### D) Player-season clusters
- `mart.player_season_clusters_modern`
- Columns:
  - `season, player_id, team_id, cluster`

---

## Archetype Feature Engineering

### Per-60 production (using TOI denominator)
- `g60`, `a60`, `p60`, `s60`

### Defensive / utility event rates
- `hit60`, `blk60`, `take60`, `give60`, `penl60`

### Faceoffs
- `fo_win_pct = faceoff_wins / faceoff_taken`

**Known issue**
- `fo_win_pct` has missingness (nulls). We observed a sizable number of nulls.
  - Must **impute** or **exclude** from clustering consistently.

### Usage / workload
- `toi_total_sec`
- `toi_per_game`
- `es_share` (share of total TOI that is even-strength)

### Possession / play-driving
- `cf60`, `ca60`, `cf_percent`  
  (Corsi is computed on even strength by construction)

---

## Clustering Results

### Model
- Unsupervised clustering on standardized features
- **k = 3** selected based on interpretability + observed structure (and prior quick scoring)

### Output size
- `mart.player_season_clusters_modern` contains ~**5007** player-season rows spanning 20182019–20242025.

### Cluster summary (observed)
You computed cluster summaries like:
- counts (n)
- average `p60`, `s60`, `hit60`, `blk60`, `fo_win_pct`, `cf_percent`, `toi_per_game`

> Keep the exact cluster-center table in a separate artifact if needed.

### Position composition sanity check (observed)
Within-cluster position mix:

| cluster | F count | D count | F % | D % |
|--------:|--------:|--------:|----:|----:|
| 0 | 854 | 526 | 61.9% | 38.1% |
| 1 | 1217 | 1121 | 52.1% | 47.9% |
| 2 | 1158 | 131 | 89.8% | 10.2% |

**Interpretation**
- Cluster 2 is strongly forward-dominant → consistent with an “offensive/play-driving” role cluster.
- Clusters 0/1 are mixed → supports role clustering beyond trivial F/D separation.

---

## Sanity Checks & Coverage

### Coverage per season (observed row counts)
Approx per-season player-season rows:
- 20182019: ~733
- 20192020: ~656
- 20202021: ~661
- 20212022: ~752
- 20222023: ~737
- 20232024: ~729
- 20242025: ~739

### Faceoff missingness
- A notable number of `fo_win_pct` values are null (observed in sanity query).
- Action: decide on one of:
  - **Impute** missing FO% (recommended: league-average or positional average)
  - Drop FO features from clustering and re-run

### Transition analysis (observed)
We observed non-trivial cluster changes for multi-season players:
- Many players do **not** remain in the same archetype across seasons.
- For players with 6+ seasons, transition-rate quantiles were approximately:
  - p10 ~ 0.45, p25 ~ 0.57, p50 ~ 0.71, p75 ~ 0.75, p90 ~ 0.86

**Interpretation**
- Archetypes act more like **states** than static labels → motivates Markov modeling.

---

## Leakage Risks & Confounding Variables

### Leakage risks (for predictive modeling)
1) **Same-game leakage**
- Never use stats from the game being predicted as model inputs.
- Fix: use rolling aggregates strictly from *prior games*.

2) **Goals/assists as near-outcomes**
- Boxscore stats are very close to the outcome variable.
- Fix: use them only as historical form features (lagged windows).

3) **Cap hit / salary leakage**
- Salary proxies player quality and deployment.
- Decision: keep salary out of clustering and primary prediction features unless explicitly modeled as a “market prior.”

4) **Special teams leakage**
- Even-strength Corsi excludes PP/PK by design (good).
- Boxscore events may reflect PP/PK usage.
- Fix: incorporate usage splits (ES share, future PP/PK shares) or compute ES-only event subsets.

### Confounders (interpretation & forecasting)
1) **Team/system effects**
- Player rates depend on team pace, tactics, and teammate quality.
- Mitigation: team baselines, relative-to-team features, hierarchical models later.

2) **Deployment effects**
- Line matching, zone starts, score effects influence observed rates.
- Mitigation: include context features; build line-level/matchup datasets.

3) **TOI threshold / survivorship bias**
- Filtering low-TOI players improves signal but biases the sample toward regulars.
- Mitigation: document threshold and sensitivity-test.

---

## How We’ll Use Archetypes Moving Forward

### A) Markov / Bayesian role transitions
- Treat `cluster` as a discrete player **state**.
- Estimate transition matrix: `P(cluster_{t+1} | cluster_t)`
- Apply Bayesian smoothing (Dirichlet prior) to avoid zero-probability transitions.
- Optionally condition transitions on:
  - position (F/D)
  - team change vs. same team
  - TOI trend
  - aging proxy (seasons played)

### B) Matchup and outcome prediction using Corsi + archetype composition
We’ll build game-level predictors like:
- TOI-weighted archetype shares per team (and eventually per line)
- rolling possession (CF60/CA60/CF%) using prior N games
- matchup deltas (home vs away archetype mix + possession differences)
- context controls (home/away, rest, score effects if available)

Then fit probabilistic models:
- Bayesian logistic regression for win probability
- or Bayesian Poisson/NegBin for goals / goal differential

Validation:
- Use **time-based splits** (train earlier seasons, test later seasons)

---

## Next Steps

1) **Finalize FO handling**
- Decide imputation strategy or drop FO from clustering.
- Re-run clusters if needed.

2) **Attach clusters across all seasons consistently**
- Avoid “single-season join” artifacts that can distort transition metrics.

3) **Line-level / matchup dataset**
- Build line compositions using TOI-based lineup approximations or top EV TOI skaters per game.
- Compute archetype mixes per line and line-vs-line matchup deltas.

4) **Model build plan**
- Start with team-level game outcome prediction (simpler baseline)
- Then extend to line-level matchup modeling.

5) **Documentation**
- Update README database section (AWS RDS access) and link this file.
