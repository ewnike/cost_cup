from functools import lru_cache

import dash
import numpy as np
import pandas as pd
import plotly.express as px
from dash import Input, Output, State, callback_context, ctx, dash_table, dcc, html
from dash.dash_table.Format import Format, Scheme
from sqlalchemy import text

from db_utils import get_db_engine

dash.register_page(__name__, path="/tab-3", name="Tab 3 — Team What-If", order=3)


# ---------------- SQL ----------------
SQL_SEASONS = """
SELECT DISTINCT season
FROM mart.v_player_season_archetypes_modern_regulars
ORDER BY season;
"""

SQL_TEAMS_BY_SEASON = """
SELECT DISTINCT team_code
FROM mart.v_player_season_archetypes_modern_regulars
WHERE season = :season
ORDER BY team_code;
"""

SQL_TEAM_ROSTER_ARCH = """
SELECT
  season,
  team_code,
  pos_group,
  cluster,
  player_id,
  toi_es_sec,
  cluster_toi_total_sec,
  toi_per_game,
  es_net60
FROM mart.v_player_season_archetypes_modern_regulars
WHERE season = :season
  AND team_code = :team_code
ORDER BY pos_group, toi_es_sec DESC;
"""

SQL_ADD_CANDIDATES = """
SELECT
  season,
  team_code,
  pos_group,
  cluster,
  player_id,
  toi_es_sec,
  NULL::bigint AS cluster_toi_total_sec,
  toi_per_game,
  es_net60
FROM mart.v_player_season_archetypes_modern_regulars
WHERE season = :season
  AND player_id != ALL(COALESCE(CAST(:exclude_pids AS bigint[]), ARRAY[]::bigint[]))
ORDER BY toi_es_sec DESC
LIMIT 3000;
"""

SQL_TRANS_F = """
SELECT from_cluster, to_cluster, prob_mean
FROM mart.cluster_transitions_modern_f
ORDER BY from_cluster, to_cluster;
"""

SQL_TRANS_D = """
SELECT from_cluster, to_cluster, prob_mean
FROM mart.cluster_transitions_modern_d
ORDER BY from_cluster, to_cluster;
"""


SQL_MODEL_PROBS_BY_SEASON = """
SELECT
  pos_group,
  season_t,
  player_id,
  p_to0, p_to1, p_to2
FROM mart.v_cluster_transition_model_probs_modern
WHERE season_t = :season_t;
"""

SQL_MODEL_PROBS_F = """
SELECT season_t, player_id, p_to0, p_to1, p_to2
FROM mart.cluster_transition_model_probs_f
WHERE season_t = :season_t;
"""

SQL_MODEL_PROBS_D = """
SELECT season_t, player_id, p_to0, p_to1, p_to2
FROM mart.cluster_transition_model_probs_d
WHERE season_t = :season_t;
"""

MODEL_MAP_CACHE: dict[int, tuple[dict, dict]] = {}

TAB3_GLOSSARY = [
    {
        "term": "ES net60",
        "definition": (
            "Even-strength net shot attempts per 60 minutes. "
            "Formula: ES net60 = CF60 − CA60 (5v5 / even strength). "
            "Positive means more shot attempts for than against (territorial advantage)."
        ),
    },
    {
        "term": "CF60",
        "definition": (
            "Corsi For per 60 at even strength: team shot attempts for per 60 minutes "
            "(shots + misses + blocks, depending on definition)."
        ),
    },
    {
        "term": "CA60",
        "definition": (
            "Corsi Against per 60 at even strength: opponent shot attempts per 60 minutes "
            "while the player/team is on ice."
        ),
    },
    {
        "term": "Why Corsi rates matter",
        "definition": (
            "Corsi rates are a common proxy for puck possession / territorial advantage. "
            "They’re not goals, but they correlate with driving play."
        ),
    },
    {
        "term": "KPI: Before",
        "definition": (
            "Expected team ES net60 for the selected roster as-is (no what-if move applied)."
        ),
    },
    {
        "term": "KPI: After",
        "definition": (
            "Expected team ES net60 after applying the what-if move (remove/add player)."
        ),
    },
    {
        "term": "KPI: Δ (delta)",
        "definition": (
            "Directional impact estimate: Δ = After − Before. Positive means the move "
            "improves expected ES net60; negative means it worsens."
        ),
    },
    {
        "term": "Role mode: Current",
        "definition": (
            "Uses current-season cluster assignments/roles for the roster (what the team is now)."
        ),
    },
    {
        "term": "Role mode: Projected",
        "definition": (
            "Estimates next-season role distribution using transition probabilities "
            "(how roles tend to change season-to-season)."
        ),
    },
    {
        "term": "Fallback behavior",
        "definition": (
            "If a player doesn’t have supervised model probabilities in the DB for that season, "
            "we fall back to the Dirichlet-smoothed transition matrix for that pos_group and from_cluster."
        ),
    },
    {
        "term": "Cluster (0/1/2)",
        "definition": (
            "A 3-bucket archetype label from the clustering model. "
            "0/1/2 are just IDs (not ranks). Each cluster represents a different player role/style "
            "based on the features used in the model."
        ),
    },
    {
        "term": "Cluster meaning (recommended labels)",
        "definition": (
            "Use labels in the UI so users don’t have to remember numbers. Example mapping:\n"
            "Cluster 0 = Defensive / low-event\n"
            "Cluster 1 = Balanced / two-way\n"
            "Cluster 2 = Offensive / high-event\n"
            "(Confirm these labels match how your model clusters behave.)"
        ),
    },
    {
        "term": "Ranking / sorting",
        "definition": (
            "Tables are sorted to show the most relevant rows first. "
            "Roster table is sorted by pos_group then ES TOI (toi_es_sec) descending, "
            "so the biggest even-strength contributors appear at the top."
        ),
    },
    {
        "term": "Transition matrix (from_cluster → to_cluster)",
        "definition": (
            "Probabilities for how players historically move between clusters from one season to the next. "
            "Rows sum to ~1. Used as a fallback when model probabilities aren’t available."
        ),
    },
]


# ---------------- helpers ----------------
@lru_cache(maxsize=1)
def get_engine():
    return get_db_engine()


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    eng = get_engine()
    return pd.read_sql_query(text(sql), eng, params=params or {})


def season_next(season: int) -> int:
    """NHL season encoding: 20182019 -> 20192020 (add 10001)."""
    return int(season) + 10001


def season_label(season: int) -> str:
    s = int(season)
    y1 = s // 10000
    y2 = s % 10000
    return f"{y1}-{str(y2)[-2:]}"


def load_model_maps_for_season(season_t: int):
    """Return (MODEL_MAP_F, MODEL_MAP_D) for a season_t, cached."""
    season_t = int(season_t)
    if season_t in MODEL_MAP_CACHE:
        return MODEL_MAP_CACHE[season_t]

    df_f = read_df(SQL_MODEL_PROBS_F, {"season_t": season_t})
    df_d = read_df(SQL_MODEL_PROBS_D, {"season_t": season_t})

    # build fast lookup dicts
    map_f = {
        (int(r.season_t), int(r.player_id)): (
            float(r.p_to0),
            float(r.p_to1),
            float(r.p_to2),
        )
        for r in df_f.itertuples(index=False)
    }
    map_d = {
        (int(r.season_t), int(r.player_id)): (
            float(r.p_to0),
            float(r.p_to1),
            float(r.p_to2),
        )
        for r in df_d.itertuples(index=False)
    }

    MODEL_MAP_CACHE[season_t] = (map_f, map_d)
    return map_f, map_d


def load_transition_probs(pos_group: str) -> dict[int, dict[int, float]]:
    """
    Return mapping.

    Returns mappingprobs[from_cluster][to_cluster] = prob_mean
    pos_group: 'F' or 'D'
    """
    pos = pos_group.upper()
    if pos not in {"F", "D"}:
        raise ValueError(f"pos_group must be 'F' or 'D', got: {pos_group}")

    sql = SQL_TRANS_F if pos == "F" else SQL_TRANS_D
    df = read_df(sql)

    probs: dict[int, dict[int, float]] = {0: {}, 1: {}, 2: {}}
    for r in df.itertuples(index=False):
        probs[int(r.from_cluster)][int(r.to_cluster)] = float(r.prob_mean)

    # guardrail: ensure full 3x3
    for fc in (0, 1, 2):
        for tc in (0, 1, 2):
            probs.setdefault(fc, {}).setdefault(tc, 0.0)

    return probs


# ✅ global cache (loaded once at app startup)
TRANS_F = load_transition_probs("F")
TRANS_D = load_transition_probs("D")


def _check_trans(T, name):
    for fc in (0, 1, 2):
        row_sum = sum(T[fc][tc] for tc in (0, 1, 2))
        assert abs(row_sum - 1.0) < 1e-6, f"{name} row {fc} sums to {row_sum}"


_check_trans(TRANS_F, "TRANS_F")
_check_trans(TRANS_D, "TRANS_D")


def compute_composition(df: pd.DataFrame, weighting: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["pos_group", "cluster", "w", "pct"])

    if weighting == "toi":
        agg = df.groupby(["pos_group", "cluster"], as_index=False)["toi_es_sec"].sum()
        agg = agg.rename(columns={"toi_es_sec": "w"})
    else:
        agg = (
            df.groupby(["pos_group", "cluster"], as_index=False)
            .size()
            .rename(columns={"size": "w"})
        )

    totals = agg.groupby("pos_group", as_index=False)["w"].sum().rename(columns={"w": "w_pos"})
    out = agg.merge(totals, on="pos_group", how="left")
    out["pct"] = (100.0 * out["w"] / out["w_pos"]).round(2)
    return out.sort_values(["pos_group", "cluster"])


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    m = v.notna() & w.notna() & (w > 0)
    if not m.any():
        return 0.0
    return float((v[m] * w[m]).sum() / w[m].sum())


def compute_expected_composition(
    df_roster: pd.DataFrame,
    weighting: str,
    trans_f: dict[int, dict[int, float]],
    trans_d: dict[int, dict[int, float]],
) -> pd.DataFrame:
    """
    Projected composition using ONLY the Dirichlet-smoothed transition matrices (TRANS_F/TRANS_D).

    Returns columns: pos_group, cluster, w, pct
    - weighting="toi" uses toi_es_sec as weights
    - otherwise uses player counts
    """
    if df_roster.empty:
        return pd.DataFrame(columns=["pos_group", "cluster", "w", "pct"])

    df = df_roster.copy()

    # player weights
    if weighting == "toi":
        df["w_player"] = pd.to_numeric(df["toi_es_sec"], errors="coerce").fillna(0.0)
    else:
        df["w_player"] = 1.0

    rows = []
    for r in df.itertuples(index=False):
        pos = str(r.pos_group).upper()
        from_c = int(r.cluster)
        w = float(r.w_player)

        probs = trans_f if pos == "F" else trans_d  # each row: {0:p0,1:p1,2:p2}
        rows.append({"pos_group": pos, "cluster": 0, "w": w * float(probs[from_c][0])})
        rows.append({"pos_group": pos, "cluster": 1, "w": w * float(probs[from_c][1])})
        rows.append({"pos_group": pos, "cluster": 2, "w": w * float(probs[from_c][2])})

    exp_df = pd.DataFrame(rows)

    agg = exp_df.groupby(["pos_group", "cluster"], as_index=False)["w"].sum()
    totals = agg.groupby("pos_group", as_index=False)["w"].sum().rename(columns={"w": "w_pos"})
    out = agg.merge(totals, on="pos_group", how="left")
    out["pct"] = (100.0 * out["w"] / out["w_pos"]).round(2)

    return out.sort_values(["pos_group", "cluster"])


def compute_expected_composition_model(
    df_roster: pd.DataFrame,
    weighting: str,
    season_t: int,
    trans_f: dict[int, dict[int, float]],
    trans_d: dict[int, dict[int, float]],
    model_map_f: dict[tuple[int, int], tuple[float, float, float]],
    model_map_d: dict[tuple[int, int], tuple[float, float, float]],
) -> pd.DataFrame:
    if df_roster.empty:
        return pd.DataFrame(columns=["pos_group", "cluster", "w", "pct"])

    df = df_roster.copy()

    # weights per player
    if weighting == "toi":
        df["w_player"] = pd.to_numeric(df["toi_es_sec"], errors="coerce").fillna(0.0)
    else:
        df["w_player"] = 1.0

    used_model = 0
    used_backoff = 0
    missing_pos = 0
    skipped_bad_cluster = 0

    rows: list[dict] = []

    for r in df.itertuples(index=False):
        pos = str(getattr(r, "pos_group", "")).upper()
        if pos not in ("F", "D"):
            missing_pos += 1
            continue

        pid = int(getattr(r, "player_id"))
        w = float(getattr(r, "w_player"))

        # cluster guardrail
        from_c_raw = getattr(r, "cluster", None)
        if from_c_raw is None or pd.isna(from_c_raw):
            skipped_bad_cluster += 1
            continue

        from_c = int(from_c_raw)
        if from_c not in (0, 1, 2):
            skipped_bad_cluster += 1
            continue

        # model probs lookup + fallback transition row
        key = (int(season_t), pid)

        if pos == "F":
            p = model_map_f.get(key)
            backoff = trans_f[from_c]
        else:
            p = model_map_d.get(key)
            backoff = trans_d[from_c]

        # choose probs
        use_backoff = True
        if p is not None:
            try:
                p0, p1, p2 = float(p[0]), float(p[1]), float(p[2])
                s = p0 + p1 + p2
                if np.isfinite(p0) and np.isfinite(p1) and np.isfinite(p2) and s > 0:
                    use_backoff = False
            except Exception:
                use_backoff = True

        if use_backoff:
            used_backoff += 1
            p0, p1, p2 = float(backoff[0]), float(backoff[1]), float(backoff[2])
        else:
            used_model += 1
            s = p0 + p1 + p2
            p0, p1, p2 = p0 / s, p1 / s, p2 / s

        rows.append({"pos_group": pos, "cluster": 0, "w": w * p0})
        rows.append({"pos_group": pos, "cluster": 1, "w": w * p1})
        rows.append({"pos_group": pos, "cluster": 2, "w": w * p2})

    print(
        f"[expected_comp] season_t={season_t} used_model={used_model} "
        f"used_backoff={used_backoff} missing_pos={missing_pos} "
        f"skipped_bad_cluster={skipped_bad_cluster} total_players={len(df)}"
    )

    exp_df = pd.DataFrame(rows)
    if exp_df.empty:
        return pd.DataFrame(columns=["pos_group", "cluster", "w", "pct"])

    agg = exp_df.groupby(["pos_group", "cluster"], as_index=False)["w"].sum()
    totals = agg.groupby("pos_group", as_index=False)["w"].sum().rename(columns={"w": "w_pos"})
    out = agg.merge(totals, on="pos_group", how="left")
    out["pct"] = (100.0 * out["w"] / out["w_pos"]).round(2)
    return out.sort_values(["pos_group", "cluster"])


def kpi_box(label: str, value: float, color: str | None = None) -> html.Div:
    style = {
        "border": "1px solid #ddd",
        "borderRadius": "10px",
        "padding": "10px",
        "minWidth": "180px",
    }
    if color:
        style["border"] = f"2px solid {color}"

    return html.Div(
        [
            html.Div(label, style={"fontSize": "12px", "opacity": 0.8}),
            html.Div(
                f"{value:+.3f}" if "Δ" in label else f"{value:.3f}",
                style={"fontSize": "22px", "fontWeight": "600"},
            ),
        ],
        style=style,
    )


def apply_whatif(df_roster, df_add_pool, remove_pid, add_pid):
    out = df_roster.copy()
    out["player_id"] = out["player_id"].astype("int64")

    df_add_pool = df_add_pool.copy()
    df_add_pool["player_id"] = df_add_pool["player_id"].astype("int64")

    remove_pid = int(remove_pid) if remove_pid is not None else None
    add_pid = int(add_pid) if add_pid is not None else None

    if remove_pid is not None:
        out = out[out["player_id"] != remove_pid].copy()

    if add_pid is not None:
        cand = df_add_pool[df_add_pool["player_id"] == add_pid]
        if not cand.empty:
            cand = cand.iloc[[0]].copy()
            cand = cand.reindex(columns=out.columns)
            out = pd.concat([out, cand], ignore_index=True)

    return out


def make_comp_fig(df_comp: pd.DataFrame, title: str):
    if df_comp.empty:
        return px.bar(title=title)

    dfp = df_comp.copy()
    dfp["cluster"] = dfp["cluster"].astype(int).astype(str)

    fig = px.bar(
        dfp,
        x="pos_group",
        y="pct",
        color="cluster",
        barmode="stack",
        text="pct",
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="inside")
    fig.update_layout(yaxis_title="% within pos_group", xaxis_title="pos_group")
    return fig


@lru_cache(maxsize=2)
def get_center_net60(pos_group: str) -> dict[int, float]:
    return load_center_net60(pos_group)


def load_center_net60(pos_group: str) -> dict[int, float]:
    sql = """
    SELECT cluster, AVG((cf60 - ca60))::float8 AS net60
    FROM mart.v_player_season_archetypes_modern_regulars
    WHERE pos_group = :pos_group
    GROUP BY 1
    ORDER BY 1;
    """
    dfc = read_df(sql, {"pos_group": pos_group})
    out = {0: 0.0, 1: 0.0, 2: 0.0}
    for r in dfc.itertuples(index=False):
        out[int(r.cluster)] = float(r.net60)
    return out


def compute_expected_net60(df_roster: pd.DataFrame, weighting: str, season: int) -> float:
    if df_roster.empty:
        return 0.0

    # player weights
    w = (
        pd.to_numeric(df_roster["toi_es_sec"], errors="coerce").fillna(0.0)
        if weighting == "toi"
        else pd.Series(1.0, index=df_roster.index)
    )

    exp_vals = []
    exp_wts = []

    for r, wi in zip(df_roster.itertuples(index=False), w):
        pos = str(r.pos_group)
        from_c = int(r.cluster)
        wi = float(wi)

        # transition probs for that player’s current cluster
        probs = TRANS_F if pos == "F" else TRANS_D

        # expected next-season ES net60 = sum_k p(k|from_c) * center_net60[pos,k]
        # so we need centers per pos/cluster (see below)
        center_f = get_center_net60("F")
        center_d = get_center_net60("D")
        centers = center_f if pos == "F" else center_d
        exp_net = sum(probs[from_c][k] * centers[k] for k in (0, 1, 2))

        exp_vals.append(exp_net)
        exp_wts.append(wi)

    return weighted_mean(pd.Series(exp_vals), pd.Series(exp_wts))


def compute_expected_net60_model(
    df_roster_with_probs: pd.DataFrame,
    weighting: str,
    trans_f: dict[int, dict[int, float]],
    trans_d: dict[int, dict[int, float]],
    center_net60_f: dict[int, float],
    center_net60_d: dict[int, float],
) -> float:
    """
    Calculate expected next-season ES net60 for a roster.

    Uses per-player supervised model probabilities if available:
      p_to0, p_to1, p_to2  (probabilities of next cluster 0/1/2)

    Fallback:
      if any p_to* is missing/NaN for a row, use transition matrix
      based on current cluster (cluster) and pos_group.

    weighting:
      - "toi": TOI-weighted by toi_es_sec
      - else: equal-weight per player
    """
    if df_roster_with_probs.empty:
        return 0.0

    df = df_roster_with_probs.copy()

    # weights per player
    if weighting == "toi":
        w = pd.to_numeric(df["toi_es_sec"], errors="coerce").fillna(0.0)
    else:
        w = pd.Series(1.0, index=df.index)

    # ensure numeric probability columns if present
    for c in ["p_to0", "p_to1", "p_to2"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            # if the caller forgot to join probs, create NaNs so we always fallback
            df[c] = np.nan

    exp_vals: list[float] = []
    exp_wts: list[float] = []

    for r, wi in zip(df.itertuples(index=False), w):
        pos = str(getattr(r, "pos_group")).upper()
        from_c = int(getattr(r, "cluster"))
        wi = float(wi)

        # choose per-pos resources
        probs_matrix = trans_f if pos == "F" else trans_d
        centers = center_net60_f if pos == "F" else center_net60_d

        # try model probs first
        p0 = getattr(r, "p_to0")
        p1 = getattr(r, "p_to1")
        p2 = getattr(r, "p_to2")

        if pd.notna(p0) and pd.notna(p1) and pd.notna(p2):
            probs_row = {0: float(p0), 1: float(p1), 2: float(p2)}
        else:
            # fallback to Dirichlet-smoothed transition matrix
            probs_row = probs_matrix[from_c]

        exp_net = probs_row[0] * centers[0] + probs_row[1] * centers[1] + probs_row[2] * centers[2]

        exp_vals.append(float(exp_net))
        exp_wts.append(wi)

    return weighted_mean(pd.Series(exp_vals), pd.Series(exp_wts))


def compute_expected_net60_model_map(
    df_roster: pd.DataFrame,
    weighting: str,
    season_t: int,
    trans_f: dict[int, dict[int, float]],
    trans_d: dict[int, dict[int, float]],
    model_map_f: dict[tuple[int, int], tuple[float, float, float]],
    model_map_d: dict[tuple[int, int], tuple[float, float, float]],
    center_net60_f: dict[int, float],
    center_net60_d: dict[int, float],
) -> float:
    if df_roster.empty:
        return 0.0

    df = df_roster.copy()
    if weighting == "toi":
        w = pd.to_numeric(df["toi_es_sec"], errors="coerce").fillna(0.0)
    else:
        w = pd.Series(1.0, index=df.index)

    exp_vals: list[float] = []
    exp_wts: list[float] = []

    for r, wi in zip(df.itertuples(index=False), w):
        pos = str(r.pos_group).upper()
        if pos not in ("F", "D"):
            continue

        try:
            pid = int(r.player_id)
            from_c = int(r.cluster)
        except Exception:
            continue

        wi = float(wi)

        probs_matrix = trans_f if pos == "F" else trans_d
        centers = center_net60_f if pos == "F" else center_net60_d
        key = (int(season_t), pid)

        p = model_map_f.get(key) if pos == "F" else model_map_d.get(key)

        if p is not None:
            p0, p1, p2 = float(p[0]), float(p[1]), float(p[2])
            s = p0 + p1 + p2
            if s > 0:
                p0, p1, p2 = p0 / s, p1 / s, p2 / s
            else:
                p = None  # fallback

        if p is None:
            backoff = probs_matrix.get(from_c)
            if backoff is None:
                p0 = p1 = p2 = 1.0 / 3.0
            else:
                p0, p1, p2 = float(backoff[0]), float(backoff[1]), float(backoff[2])

        c0 = float(centers.get(0, 0.0))
        c1 = float(centers.get(1, 0.0))
        c2 = float(centers.get(2, 0.0))

        exp_vals.append(p0 * c0 + p1 * c1 + p2 * c2)
        exp_wts.append(wi)

    # ✅ guards MUST be after the loop
    if not exp_vals:
        return 0.0

    total_w = float(np.nansum(exp_wts))
    if total_w <= 0:
        return float(np.nanmean(exp_vals))

    return weighted_mean(pd.Series(exp_vals), pd.Series(exp_wts))


# ---------------- app ----------------
weight_options = [
    {"label": "TOI-weighted (ES TOI)", "value": "toi"},
    {"label": "Player-seasons (counts)", "value": "player_season"},
]

role_mode_options = [
    {"label": "Current roles (this season)", "value": "current"},
    {"label": "Projected roles (next season)", "value": "projected"},
]


def layout():
    try:
        # ---------- DB work happens here (NOT at import time) ----------
        df_seasons = read_df(SQL_SEASONS)
        season_vals = [int(s) for s in df_seasons["season"].tolist()]
        season_options = [{"label": season_label(s), "value": s} for s in season_vals]
        default_season = season_vals[-1] if season_vals else 20242025

        df_teams0 = read_df(SQL_TEAMS_BY_SEASON, {"season": default_season})
        team_vals = df_teams0["team_code"].tolist()
        team_options0 = [{"label": t, "value": t} for t in team_vals]
        default_team = team_vals[0] if team_vals else None

        # ---------- controls row ----------
        controls_row = html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "1fr 1.2fr auto",
                "gap": "12px",
                "alignItems": "center",
                "marginTop": "8px",
            },
            children=[
                # left: season + team
                html.Div(
                    style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
                    children=[
                        html.Div(
                            style={"minWidth": "200px"},
                            children=[
                                html.Label("Season"),
                                dcc.Dropdown(
                                    id="tab3-season",
                                    options=season_options,
                                    value=default_season,
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "200px"},
                            children=[
                                html.Label("Team"),
                                dcc.Dropdown(
                                    id="tab3-team_code",
                                    options=team_options0,
                                    value=default_team,
                                    clearable=False,
                                ),
                            ],
                        ),
                    ],
                ),
                # middle: radios
                html.Div(
                    style={"display": "flex", "flexDirection": "column", "gap": "8px"},
                    children=[
                        html.Div(
                            children=[
                                html.Label("Weighting"),
                                dcc.RadioItems(
                                    id="weighting",
                                    options=weight_options,
                                    value="toi",
                                    inline=True,
                                ),
                            ]
                        ),
                        html.Div(
                            children=[
                                html.Label("Role mode"),
                                dcc.RadioItems(
                                    id="role_mode",
                                    options=role_mode_options,
                                    value="current",
                                    inline=True,
                                ),
                            ]
                        ),
                    ],
                ),
                # right: glossary button
                html.Div(
                    style={"display": "flex", "justifyContent": "flex-end"},
                    children=[
                        html.Button(
                            "Glossary / Definitions",
                            id="tab3-open_glossary",
                            n_clicks=0,
                            style={
                                "padding": "8px 12px",
                                "border": "1px solid #bbb",
                                "borderRadius": "10px",
                                "background": "#f8f8f8",
                                "cursor": "pointer",
                                "whiteSpace": "nowrap",
                            },
                        )
                    ],
                ),
            ],
        )

        # ---------- page layout ----------
        return html.Div(
            style={"maxWidth": "1200px", "margin": "0 auto", "padding": "18px"},
            children=[
                html.H2("Team Archetype Composition + What-if (V1)"),
                controls_row,
                # --- glossary modal ---
                html.Div(
                    id="tab3-glossary_modal",
                    style={
                        "display": "none",
                        "position": "fixed",
                        "top": 0,
                        "left": 0,
                        "width": "100%",
                        "height": "100%",
                        "backgroundColor": "rgba(0,0,0,0.45)",
                        "zIndex": 9999,
                        "padding": "60px 20px",
                    },
                    children=[
                        html.Div(
                            style={
                                "maxWidth": "900px",
                                "margin": "0 auto",
                                "backgroundColor": "white",
                                "borderRadius": "10px",
                                "padding": "16px 16px",
                                "boxShadow": "0 6px 24px rgba(0,0,0,0.2)",
                            },
                            children=[
                                html.Div(
                                    style={
                                        "display": "flex",
                                        "justifyContent": "space-between",
                                        "alignItems": "center",
                                    },
                                    children=[
                                        html.H3("Quick Definitions", style={"margin": 0}),
                                        html.Button(
                                            "Close",
                                            id="tab3-close_glossary",
                                            n_clicks=0,
                                        ),
                                    ],
                                ),
                                html.Hr(),
                                dcc.Input(
                                    id="tab3-glossary_search",
                                    type="text",
                                    placeholder="Search definitions…",
                                    style={"width": "360px", "padding": "6px"},
                                ),
                                html.Div(style={"height": "10px"}),
                                dash_table.DataTable(
                                    id="tab3-glossary_table",
                                    columns=[
                                        {"name": "term", "id": "term"},
                                        {"name": "definition", "id": "definition"},
                                    ],
                                    data=TAB3_GLOSSARY,
                                    page_size=10,
                                    style_cell={
                                        "whiteSpace": "normal",
                                        "height": "auto",
                                        "fontSize": 13,
                                        "padding": "8px",
                                    },
                                    style_table={"overflowX": "auto"},
                                ),
                            ],
                        )
                    ],
                ),
                html.Hr(),
                # BEFORE/AFTER charts
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px"},
                    children=[
                        dcc.Graph(id="comp_graph_before"),
                        dcc.Graph(id="comp_graph_after"),
                    ],
                ),
                # KPI row
                html.Div(id="kpi_row", style={"marginTop": "8px"}),
                html.Div(style={"height": "12px"}),
                # tables row
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px"},
                    children=[
                        # Left: roster
                        html.Div(
                            children=[
                                html.H3("Roster (regulars)"),
                                dash_table.DataTable(
                                    id="roster_table",
                                    columns=[
                                        {"name": "pos_group", "id": "pos_group"},
                                        {"name": "cluster", "id": "cluster"},
                                        {"name": "player_id", "id": "player_id"},
                                        {"name": "toi_es_min", "id": "toi_es_min"},
                                        {"name": "toi_pg", "id": "toi_pg"},
                                        {"name": "es_net60", "id": "es_net60"},
                                    ],
                                    page_size=15,
                                    sort_action="native",
                                    style_table={"overflowX": "auto"},
                                    style_cell={
                                        "fontFamily": "Arial",
                                        "fontSize": 12,
                                        "padding": "6px",
                                    },
                                ),
                            ]
                        ),
                        # Right: what-if + delta table
                        html.Div(
                            style={"marginTop": "20px", "paddingLeft": "50px"},
                            children=[
                                html.H3(
                                    "What-If Roster Move (Trade Prototype)",
                                    style={"margin": "0 0 6px 0"},
                                ),
                                html.Div(
                                    "Remove one player from this team’s roster and add one league regular (same position group).",
                                    style={
                                        "fontSize": "12px",
                                        "color": "#666",
                                        "margin": "0 0 10px 0",
                                    },
                                ),
                                html.Div(
                                    style={
                                        "display": "flex",
                                        "gap": "10px",
                                        "flexWrap": "wrap",
                                        "alignItems": "end",
                                    },
                                    children=[
                                        html.Div(
                                            style={"minWidth": "260px"},
                                            children=[
                                                html.Label("Send away (remove from this team)"),
                                                dcc.Dropdown(
                                                    id="remove_player",
                                                    clearable=True,
                                                    placeholder="Select a player to remove…",
                                                ),
                                            ],
                                        ),
                                        html.Div(
                                            style={"minWidth": "380px"},
                                            children=[
                                                html.Label("Acquire (add from league regulars)"),
                                                dcc.Dropdown(
                                                    id="add_player",
                                                    clearable=True,
                                                    placeholder="Select a player to add…",
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                                html.Br(),
                                dash_table.DataTable(
                                    id="whatif_table",
                                    columns=[
                                        {"name": "pos_group", "id": "pos_group"},
                                        {"name": "cluster", "id": "cluster"},
                                        {
                                            "name": "pct_before",
                                            "id": "pct_before",
                                            "type": "numeric",
                                            "format": Format(precision=2, scheme=Scheme.fixed),
                                        },
                                        {
                                            "name": "pct_after",
                                            "id": "pct_after",
                                            "type": "numeric",
                                            "format": Format(precision=2, scheme=Scheme.fixed),
                                        },
                                        {
                                            "name": "delta_pct",
                                            "id": "delta_pct",
                                            "type": "numeric",
                                            "format": Format(precision=2, scheme=Scheme.fixed),
                                        },
                                    ],
                                    page_size=12,
                                    sort_action="native",
                                    style_table={"overflowX": "auto"},
                                    style_cell={
                                        "fontFamily": "Arial",
                                        "fontSize": 12,
                                        "padding": "6px",
                                    },
                                    style_data_conditional=[
                                        {
                                            "if": {
                                                "filter_query": "{delta_pct} > 0",
                                                "column_id": "delta_pct",
                                            },
                                            "fontWeight": "bold",
                                        },
                                        {
                                            "if": {
                                                "filter_query": "{delta_pct} < 0",
                                                "column_id": "delta_pct",
                                            },
                                            "fontWeight": "bold",
                                        },
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

    except Exception as e:
        return html.Div(
            style={"padding": "18px"},
            children=[
                html.H3("Tab 3 failed to load"),
                html.Pre(str(e)),
            ],
        )


@dash.callback(
    Output("tab3-glossary_modal", "style"),
    Input("tab3-open_glossary", "n_clicks"),
    Input("tab3-close_glossary", "n_clicks"),
    State("tab3-glossary_modal", "style"),
)
def tab3_toggle_modal(open_n, close_n, style):
    ctx = callback_context
    if not ctx.triggered:
        return style

    trig = ctx.triggered[0]["prop_id"].split(".")[0]
    new_style = dict(style or {})

    if trig == "tab3-open_glossary":
        new_style["display"] = "block"
    elif trig == "tab3-close_glossary":
        new_style["display"] = "none"

    return new_style


@dash.callback(
    Output("tab3-glossary_table", "data"),
    Input("tab3-glossary_search", "value"),
)
def tab3_filter_glossary(q):
    if not q:
        return TAB3_GLOSSARY
    q = q.lower().strip()
    return [r for r in TAB3_GLOSSARY if q in r["term"].lower() or q in r["definition"].lower()]


def refresh_teams(season: int):
    if season is None:
        return [], None
    df = read_df(SQL_TEAMS_BY_SEASON, {"season": int(season)})
    opts = [{"label": t, "value": t} for t in df["team_code"].tolist()]
    val = opts[0]["value"] if opts else None
    return opts, val


@dash.callback(
    Output("comp_graph_before", "figure"),
    Output("comp_graph_after", "figure"),
    Output("roster_table", "data"),
    Output("remove_player", "options"),
    Output("remove_player", "value"),  # NEW
    Output("add_player", "options"),
    Output("add_player", "value"),
    Output("whatif_table", "data"),
    Output("kpi_row", "children"),
    Input("tab3-season", "value"),
    Input("tab3-team_code", "value"),
    Input("weighting", "value"),
    Input("role_mode", "value"),
    Input("remove_player", "value"),
    Input("add_player", "value"),
)
def refresh(season, team_code, weighting, role_mode, remove_pid, add_pid):
    """
    Tab 3 callback.

    Returns:
      fig_before, fig_after, roster_records, remove_opts, add_opts, whatif_delta_records, kpi_children

    """
    empty_fig = px.bar(title="")
    if season is None or team_code is None:
        return empty_fig, empty_fig, [], [], None, [], None, [], []

    season_t = int(season)

    trigger = getattr(ctx, "triggered_id", None)
    print(
        f"[TRIGGERED] {trigger} | role_mode={role_mode} season={season_t} team={team_code} w={weighting}"
    )
    print(f"[VALS] remove={remove_pid} add={add_pid}")

    # ------------------- load roster -------------------
    df = read_df(SQL_TEAM_ROSTER_ARCH, {"season": season_t, "team_code": str(team_code)})
    if df.empty:
        no_fig = px.bar(title="No roster rows")
        return no_fig, no_fig, [], [], None, [], None, [], []

    # ✅ dtype guardrails for anything used in projections / joins
    df = df.copy()
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("int64")
    df["cluster"] = pd.to_numeric(df["cluster"], errors="coerce").astype("int64")
    df["toi_es_sec"] = pd.to_numeric(df["toi_es_sec"], errors="coerce")
    df["toi_per_game"] = pd.to_numeric(df["toi_per_game"], errors="coerce")
    df["es_net60"] = pd.to_numeric(df["es_net60"], errors="coerce")
    df["pos_group"] = df["pos_group"].astype(str)

    # ------------------- roster table -------------------
    roster = df.copy()
    roster["toi_es_min"] = (pd.to_numeric(roster["toi_es_sec"], errors="coerce") / 60.0).round(1)
    roster["toi_pg"] = pd.to_numeric(roster["toi_per_game"], errors="coerce").round(2)
    roster["es_net60"] = pd.to_numeric(roster["es_net60"], errors="coerce").round(2)

    roster_records = roster[
        ["pos_group", "cluster", "player_id", "toi_es_min", "toi_pg", "es_net60"]
    ].to_dict("records")

    roster_pids = sorted(roster["player_id"].astype("int64").unique().tolist())
    remove_opts = [{"label": str(pid), "value": int(pid)} for pid in roster_pids]

    # ------------------- add candidates pool -------------------
    df_add_pool = read_df(SQL_ADD_CANDIDATES, {"season": season_t, "exclude_pids": roster_pids})

    # restrict add list to same pos_group as removed player (optional but good UX)
    pos_filter = None
    if remove_pid is not None and int(remove_pid) in set(roster_pids):
        pos_filter = roster.loc[roster["player_id"] == int(remove_pid), "pos_group"].iloc[0]

    add_pool_f = df_add_pool
    if pos_filter is not None:
        add_pool_f = add_pool_f[add_pool_f["pos_group"] == pos_filter].copy()

    add_opts = [
        {
            "label": (
                f"{int(r.player_id)} ({r.team_code}) {r.pos_group} c{int(r.cluster)} "
                f"toi_es_min={round(float(r.toi_es_sec) / 60.0, 1)}"
            ),
            "value": int(r.player_id),
        }
        for r in add_pool_f.itertuples(index=False)
    ][:400]

    trigger = getattr(ctx, "triggered_id", None)

    # --- reset stale selections on team/season change ---
    if trigger in ("season", "team_code"):
        remove_pid_out = None
        add_pid_out = None
    else:
        remove_pid_out = remove_pid
        add_pid_out = add_pid

    # --- if remove changed, keep add only if still valid ---
    if trigger == "remove_player":
        valid_add_ids = {opt["value"] for opt in add_opts}
        if add_pid not in valid_add_ids:
            add_pid_out = None
    # ------------------- apply what-if once -------------------
    df_after = apply_whatif(df, df_add_pool, remove_pid_out, add_pid_out)
    print(f"[WHATIF] before_n={len(df)} after_n={len(df_after)}")

    # ✅ same guardrails for the modified roster
    df_after = df_after.copy()
    df_after["player_id"] = pd.to_numeric(df_after["player_id"], errors="coerce").astype("int64")
    df_after["cluster"] = pd.to_numeric(df_after["cluster"], errors="coerce").astype("int64")
    df_after["toi_es_sec"] = pd.to_numeric(df_after["toi_es_sec"], errors="coerce")
    df_after["toi_per_game"] = pd.to_numeric(df_after["toi_per_game"], errors="coerce")
    df_after["es_net60"] = pd.to_numeric(df_after["es_net60"], errors="coerce")
    df_after["pos_group"] = df_after["pos_group"].astype(str)

    # ------------------- composition -------------------
    if role_mode == "projected":
        # DB-backed maps (cached)
        MODEL_MAP_F, MODEL_MAP_D = load_model_maps_for_season(season_t)
        print(f"[DB MAP] season_t={season_t} F_keys={len(MODEL_MAP_F)} D_keys={len(MODEL_MAP_D)}")

        comp_before = compute_expected_composition_model(
            df, weighting, season_t, TRANS_F, TRANS_D, MODEL_MAP_F, MODEL_MAP_D
        )
        comp_after = compute_expected_composition_model(
            df_after, weighting, season_t, TRANS_F, TRANS_D, MODEL_MAP_F, MODEL_MAP_D
        )

        center_f = get_center_net60("F")
        center_d = get_center_net60("D")

        before_net = compute_expected_net60_model_map(
            df,
            weighting,
            season_t,
            TRANS_F,
            TRANS_D,
            MODEL_MAP_F,
            MODEL_MAP_D,
            center_f,
            center_d,
        )
        after_net = compute_expected_net60_model_map(
            df_after,
            weighting,
            season_t,
            TRANS_F,
            TRANS_D,
            MODEL_MAP_F,
            MODEL_MAP_D,
            center_f,
            center_d,
        )

        role_label = f"Projected → next season ({season_label(season_next(season_t))})"

    else:
        comp_before = compute_composition(df, weighting)
        comp_after = compute_composition(df_after, weighting)

        if weighting == "toi":
            before_net = weighted_mean(df["es_net60"], df["toi_es_sec"])
            after_net = weighted_mean(df_after["es_net60"], df_after["toi_es_sec"])
        else:
            before_net = float(pd.to_numeric(df["es_net60"], errors="coerce").mean())
            after_net = float(pd.to_numeric(df_after["es_net60"], errors="coerce").mean())

        role_label = f"Current season ({season_label(season_t)})"

    # ------------------- figures -------------------
    fig_before = make_comp_fig(
        comp_before,
        title=f"{team_code} — BEFORE ({role_label}, {'TOI' if weighting == 'toi' else 'counts'})",
    )
    fig_after = make_comp_fig(
        comp_after,
        title=f"{team_code} — AFTER ({role_label}, {'TOI' if weighting == 'toi' else 'counts'})",
    )

    # ------------------- KPI row -------------------
    delta_net = round(after_net - before_net, 3)
    print(f"[KPI] before={before_net:.3f} after={after_net:.3f} delta={delta_net:+.3f}")

    kpis = [
        kpi_box("ES net60 (before)", before_net),
        kpi_box("ES net60 (after)", after_net),
        kpi_box(
            "Δ ES net60",
            delta_net,
            color=("#2ca02c" if delta_net > 0 else "#d62728" if delta_net < 0 else None),
        ),
    ]

    # ------------------- delta table -------------------
    key = ["pos_group", "cluster"]
    wb = comp_before[key + ["pct"]].rename(columns={"pct": "pct_before"})
    wa = comp_after[key + ["pct"]].rename(columns={"pct": "pct_after"})
    delta = wb.merge(wa, on=key, how="outer").fillna(0)
    delta["delta_pct"] = (delta["pct_after"] - delta["pct_before"]).round(2)
    delta = delta.sort_values(["pos_group", "cluster"])

    return (
        fig_before,
        fig_after,
        roster_records,
        remove_opts,
        remove_pid_out,
        add_opts,
        add_pid_out,
        delta.to_dict("records"),
        kpis,
    )
