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
