## Cost Cup — Project Story (What we built and why)

### Goal
Hockey performance metrics are noisy and heavily influenced by **team context**, deployment, and game state.
Instead of modeling players as a single continuous “skill value,” this project models **player roles (archetypes)** and
how players **transition between roles** across seasons.

This enables questions like:
- What archetype mix does a team have this season?
- If we swap one player for another, how does the role composition change?
- Given a player’s current archetype, what is the probability they shift archetypes next season?

### Key idea (Theory)
**Player “roles” are more stable and more predictive than raw performance**, especially when we focus on
**even-strength (ES)** play and rate-based measures. We designed features to reduce confounding from:
- special teams (PP/PK),
- ice-time inflation (TOI bias),
- star/salary deployment effects,
- game score / extreme event environments.

### Pipeline overview (end-to-end)
Raw data is transformed into stable, queryable tables that drive both modeling and the dashboard.


### Modeling approach
#### Unsupervised learning: Archetype discovery (clustering)
We cluster player-season feature vectors into **3 archetypes** per position group (F/D).  
The cluster IDs (0/1/2) are **labels**, not ranks. We optionally map them to readable names:

- 0 — Defensive / low-event  
- 1 — Balanced / two-way  
- 2 — Offensive / high-event

(Cluster interpretations are validated by comparing feature profiles across clusters.)

#### Supervised/statistical modeling: Transition probabilities
After each player-season is assigned a cluster, we model how players move between clusters across seasons:
- input: current-season cluster (and/or features)
- target: next-season cluster (0/1/2)
- output: probabilities `p_to0`, `p_to1`, `p_to2`

### Why smoothing (Dirichlet shrinkage) is necessary
Raw transition counts can produce brittle probabilities:
- rare transitions appear as 0% (even if plausible),
- small sample sizes yield overconfident estimates.

We apply Dirichlet/Bayesian smoothing:
- treat observed counts as evidence,
- add prior pseudo-counts,
- shrink extreme probabilities toward a reasonable baseline.

This produces more stable, interpretable transition probabilities suitable for reporting and dashboards.

### Data quality checks
We validate:
- key integrity (no nulls in game_id/player_id/team_id),
- no negative TOI or event counts,
- consistent team-code mapping (including franchise changes),
- coverage spot checks vs shift data.

### Deliverables
- SQL-backed feature tables and archetype assignments
- smoothed transition probabilities
- Dash application for team composition, player gamelogs, and what-if analysis

> Note: Database credentials and environment-specific deployment settings are intentionally excluded from this repo.
> Access to hosted data is provided separately when required.


## Data validation and an important edge case (team context matters)

Before final modeling, we ran targeted SQL sanity checks to validate table integrity (keys, joins, nulls, duplicates, and impossible values) and to identify edge cases that could distort interpretation.

### Sanity checks (example: 2024–2025 player-game truth table)

We verified the core player-game feature table (`mart.player_game_features_20242025_truth`) is structurally sound:

- No duplicate player-games: `rows = distinct(game_id, player_id) = 47224`
- No null key fields: `game_id_null = 0`, `player_id_null = 0`, `team_id_null = 0`
- Referential join works: `team_join_miss = 0` when joining to `dim.dim_team_code`
- No impossible values: `neg_toi = 0`, `neg_events = 0`

These checks ensure downstream visualizations and models operate on consistent, joinable data.

### Edge case: extreme CA is often a “team environment” event (not a single-player signal)

A recurring modeling risk is misinterpreting extreme values (e.g., very high CA / Corsi Against) as purely individual-player signal. To evaluate whether extreme CA reflects individual performance or team environment, we used a diagnostic query that:

1) ranks team-games by total **team CA** (sum of all skater CA on that team in that game), then  
2) lists the **top 5 individual CA** contributors within each of those high-pressure games.

## Edge case: extreme CA is often team environment (ANA example)
During validation we found that very high single-game CA values can be driven by “team getting shelled” games (shared across many skaters), not isolated player effects.  
We ranked ANA games by total team CA and showed top individual CA contributors per game.

- Takeaway: interpret extreme CA in context (team total + teammate distribution), not as a standalone player-quality signal.
- This supports our use of ES-only, rate-based season summaries and capped features.

➡️ Full table + query pattern: see `docs/PROJECT_STORY.md` (Edge Case: ANA High-CA Games).

# Edge Case: ANA High-CA Games (Why Context Matters)

**What this table is:** ANA games with the highest total team CA (sum of player CA), showing the top 5 individual player CA values per game.

- `team_ca` = sum of all ANA skater CA for that game (team environment / pressure proxy)
- `player_ca` = individual player CA in the same game
- Top 5 players shown per game

<PASTE THE TABLE HERE>

### Interpretation
(Use the longer narrative paragraph you drafted.)
