# Modern sanity checks (mart.player_game_es_*)

These checks validate the **modern even-strength player-game table(s)** in `mart`:

- `mart.player_game_es_{season}` (one table per season)
- Cross-checks reference:
  - `dim.player_info`
  - `raw.game`
  - and (optionally) `raw.raw_shifts_resolved_final` for coverage comparisons

The goal is to catch:
- missing required columns / null keys
- duplicates (game_id, player_id)
- impossible values (negative TOI/CF/CA)
- referential integrity issues (missing dim.player_info / raw.game)
- coverage gaps vs shifts-resolved

---

## How to run

From repo root:

### Run all modern sanity SQL
```bash
python -m scripts.run_sql_checks --dir sql/sanity/modern