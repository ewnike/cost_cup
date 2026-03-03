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
