from pathlib import Path

import dash
import numpy as np
import pandas as pd
import plotly.express as px
from dash import Input, Output, dash_table, dcc, html
from dash import callback_context as ctx
from dash.dash_table.Format import Format, Scheme, Sign
from sqlalchemy import text

from db_utils import get_db_engine  # same import style as your other working pages

MODEL_OUT = Path("model_out")
F_MODEL_CSV = MODEL_OUT / "transition_probs_F_test_season_20232024.csv"
D_MODEL_CSV = MODEL_OUT / "transition_probs_D_test_season_20232024.csv"

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
ENGINE = get_db_engine()

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

# MODEL_MAP_CACHE: dict[
#     int,
#     tuple[
#         dict[tuple[int, int], tuple[float, float, float]],
#         dict[tuple[int, int], tuple[float, float, float]],
#     ],
# ] = {}
MODEL_MAP_CACHE: dict[int, tuple[dict, dict]] = {}


# ---------------- helpers ----------------
def season_next(season: int) -> int:
    """NHL season encoding: 20182019 -> 20192020 (add 10001)."""
    return int(season) + 10001


def load_model_maps_for_season(season_t: int):
    """Return (MODEL_MAP_F, MODEL_MAP_D) for a season_t, cached."""
    season_t = int(season_t)
    if season_t in MODEL_MAP_CACHE:
        return MODEL_MAP_CACHE[season_t]

    df_f = read_df(SQL_MODEL_PROBS_F, {"season_t": season_t})
    df_d = read_df(SQL_MODEL_PROBS_D, {"season_t": season_t})

    # build fast lookup dicts
    map_f = {
        (int(r.season_t), int(r.player_id)): (float(r.p_to0), float(r.p_to1), float(r.p_to2))
        for r in df_f.itertuples(index=False)
    }
    map_d = {
        (int(r.season_t), int(r.player_id)): (float(r.p_to0), float(r.p_to1), float(r.p_to2))
        for r in df_d.itertuples(index=False)
    }

    MODEL_MAP_CACHE[season_t] = (map_f, map_d)
    return map_f, map_d


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql_query(text(sql), ENGINE, params=params or {})
    # engine = get_db_engine()
    # try:
    #     return pd.read_sql_query(text(sql), engine, params=params or {})
    # finally:
    #     engine.dispose()


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


def load_model_probs(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        # empty DF signals “no model probs available”
        return pd.DataFrame(
            columns=["player_id", "season_t", "pos_group", "p_to0", "p_to1", "p_to2"]
        )
    df = pd.read_csv(csv_path)
    # keep only what we need, enforce types
    df = df[["player_id", "season_t", "pos_group", "p_to0", "p_to1", "p_to2"]].copy()
    df["player_id"] = df["player_id"].astype("int64")
    df["season_t"] = df["season_t"].astype("int64")
    df["pos_group"] = df["pos_group"].astype(str)
    for c in ["p_to0", "p_to1", "p_to2"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


MODEL_PROBS_F = load_model_probs(F_MODEL_CSV)
MODEL_PROBS_D = load_model_probs(D_MODEL_CSV)

# dtype guardrails (ONLY if load_model_probs doesn't already enforce them)
for dfp in (MODEL_PROBS_F, MODEL_PROBS_D):
    if dfp.empty:
        continue
    dfp["player_id"] = dfp["player_id"].astype("int64")
    dfp["season_t"] = dfp["season_t"].astype("int64")
    for c in ["p_to0", "p_to1", "p_to2"]:
        dfp[c] = dfp[c].astype("float64")

MODEL_PROBS_F_THIN = (
    MODEL_PROBS_F[["player_id", "season_t", "p_to0", "p_to1", "p_to2"]].copy()
    if not MODEL_PROBS_F.empty
    else MODEL_PROBS_F
)
MODEL_PROBS_D_THIN = (
    MODEL_PROBS_D[["player_id", "season_t", "p_to0", "p_to1", "p_to2"]].copy()
    if not MODEL_PROBS_D.empty
    else MODEL_PROBS_D
)


def to_model_prob_map(df_probs: pd.DataFrame) -> dict[tuple[int, int], tuple[float, float, float]]:
    if df_probs.empty:
        return {}
    out: dict[tuple[int, int], tuple[float, float, float]] = {}
    for r in df_probs.itertuples(index=False):
        out[(int(r.season_t), int(r.player_id))] = (float(r.p_to0), float(r.p_to1), float(r.p_to2))
    return out


MODEL_MAP_F = to_model_prob_map(MODEL_PROBS_F)
MODEL_MAP_D = to_model_prob_map(MODEL_PROBS_D)

# optional sanity (I’d keep these during dev)
assert MODEL_MAP_F, "MODEL_MAP_F is empty"
assert MODEL_MAP_D, "MODEL_MAP_D is empty"

print("MODEL_MAP_F example key:", next(iter(MODEL_MAP_F.keys())))
print("MODEL_MAP_D example key:", next(iter(MODEL_MAP_D.keys())))


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
    df["w_player"] = (
        pd.to_numeric(df["toi_es_sec"], errors="coerce").fillna(0.0) if weighting == "toi" else 1.0
    )

    used_model = 0
    used_backoff = 0
    missing_pos = 0

    rows = []
    for r in df.itertuples(index=False):
        pos = str(r.pos_group).upper()
        if pos not in ("F", "D"):
            missing_pos += 1
            continue

        pid = int(r.player_id)
        from_c = int(r.cluster)
        w = float(r.w_player)

        # pull model probs if available
        key = (season_t, pid)
        if pos == "F":
            p = model_map_f.get(key)
            backoff = trans_f[from_c]
        else:
            p = model_map_d.get(key)
            backoff = trans_d[from_c]

        use_backoff = True
        if p is not None:
            try:
                p0, p1, p2 = float(p[0]), float(p[1]), float(p[2])
                if np.isfinite(p0) and np.isfinite(p1) and np.isfinite(p2) and (p0 + p1 + p2) > 0:
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
        f"used_backoff={used_backoff} missing_pos={missing_pos} total_players={len(df)}"
    )

    exp_df = pd.DataFrame(rows)
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


CENTER_NET60_F = load_center_net60("F")
CENTER_NET60_D = load_center_net60("D")


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
        centers = CENTER_NET60_F if pos == "F" else CENTER_NET60_D
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

    if weighting == "toi":
        wts = pd.to_numeric(df_roster["toi_es_sec"], errors="coerce").fillna(0.0).to_numpy()
    else:
        wts = np.ones(len(df_roster), dtype=float)

    exp_vals = []
    exp_wts = []

    for r, wi in zip(df_roster.itertuples(index=False), wts):
        pos = str(r.pos_group).upper()
        pid = int(r.player_id)
        from_c = int(r.cluster)
        wi = float(wi)

        if pos == "F":
            p = model_map_f.get((season_t, pid))
            backoff = trans_f[from_c]
            centers = center_net60_f
        else:
            p = model_map_d.get((season_t, pid))
            backoff = trans_d[from_c]
            centers = center_net60_d

        if p is None:
            p0, p1, p2 = float(backoff[0]), float(backoff[1]), float(backoff[2])
        else:
            p0, p1, p2 = map(float, p)
            s = p0 + p1 + p2
            if s > 0:
                p0, p1, p2 = p0 / s, p1 / s, p2 / s

        exp_net = p0 * centers[0] + p1 * centers[1] + p2 * centers[2]
        exp_vals.append(exp_net)
        exp_wts.append(wi)

    return weighted_mean(pd.Series(exp_vals), pd.Series(exp_wts))


# ---------------- app ----------------
app = dash.Dash(__name__)
server = app.server

df_seasons = read_df(SQL_SEASONS)
season_options = [{"label": str(s), "value": int(s)} for s in df_seasons["season"].tolist()]
default_season = season_options[-1]["value"] if season_options else 20242025

df_teams0 = read_df(SQL_TEAMS_BY_SEASON, {"season": default_season})
team_options0 = [{"label": t, "value": t} for t in df_teams0["team_code"].tolist()]
default_team = team_options0[0]["value"] if team_options0 else None

weight_options = [
    {"label": "TOI-weighted (ES TOI)", "value": "toi"},
    {"label": "Player-seasons (counts)", "value": "player_season"},
]

role_mode_options = [
    {"label": "Current roles (this season)", "value": "current"},
    {"label": "Projected roles (next season)", "value": "projected"},
]
app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "18px"},
    children=[
        html.H2("Team Archetype Composition + What-if (V1)"),
        # Controls row
        html.Div(
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "alignItems": "end"},
            children=[
                html.Div(
                    style={"minWidth": "200px"},
                    children=[
                        html.Label("Season"),
                        dcc.Dropdown(
                            id="season",
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
                            id="team_code",
                            options=team_options0,
                            value=default_team,
                            clearable=False,
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "280px"},
                    children=[
                        html.Label("Weighting"),
                        dcc.RadioItems(
                            id="weighting",
                            options=weight_options,
                            value="toi",
                            inline=True,
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "320px"},
                    children=[
                        html.Label("Role mode"),
                        dcc.RadioItems(
                            id="role_mode",
                            options=role_mode_options,
                            value="current",
                            inline=True,
                        ),
                    ],
                ),
            ],
        ),
        html.Hr(),
        # BEFORE/AFTER charts (callback updates figures)
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px"},
            children=[
                dcc.Graph(id="comp_graph_before"),
                dcc.Graph(id="comp_graph_after"),
            ],
        ),
        # KPI row (callback populates children)
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
                            style_cell={"fontFamily": "Arial", "fontSize": 12, "padding": "6px"},
                        ),
                    ]
                ),
                # Right: what-if + delta table
                html.Div(
                    style={"marginTop": "20px", "paddingLeft": "50px"},
                    children=[
                        html.H3(
                            "What-If Roster Move (Trade Prototype)", style={"margin": "0 0 6px 0"}
                        ),
                        html.Div(
                            "Remove one player from this team’s roster and add one league regular (same position group).",
                            style={"fontSize": "12px", "color": "#666", "margin": "0 0 10px 0"},
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
                            style_cell={"fontFamily": "Arial", "fontSize": 12, "padding": "6px"},
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


@app.callback(
    Output("team_code", "options"),
    Output("team_code", "value"),
    Input("season", "value"),
)
def refresh_teams(season: int):
    if season is None:
        return [], None
    df = read_df(SQL_TEAMS_BY_SEASON, {"season": int(season)})
    opts = [{"label": t, "value": t} for t in df["team_code"].tolist()]
    val = opts[0]["value"] if opts else None
    return opts, val


@app.callback(
    Output("remove_player", "value"),
    Output("add_player", "value"),
    Input("season", "value"),
    Input("team_code", "value"),
)
def reset_moves_on_team_change(season, team_code):
    return None, None


@app.callback(
    Output("comp_graph_before", "figure"),
    Output("comp_graph_after", "figure"),
    Output("roster_table", "data"),
    Output("remove_player", "options"),
    Output("add_player", "options"),
    Output("whatif_table", "data"),
    Output("kpi_row", "children"),
    Input("season", "value"),
    Input("team_code", "value"),
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
    from dash import ctx

    empty_fig = px.bar(title="")
    if season is None or team_code is None:
        return empty_fig, empty_fig, [], [], [], [], []

    season_t = int(season)

    trig = getattr(ctx, "triggered_id", None)
    print(
        f"[TRIGGERED] {trig} | role_mode={role_mode} season={season_t} team={team_code} w={weighting}"
    )
    print(f"[VALS] remove={remove_pid} add={add_pid}")

    # ------------------- load roster -------------------
    df = read_df(SQL_TEAM_ROSTER_ARCH, {"season": season_t, "team_code": str(team_code)})
    if df.empty:
        no_fig = px.bar(title="No roster rows")
        return no_fig, no_fig, [], [], [], [], []

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

    # ------------------- apply what-if once -------------------
    df_after = apply_whatif(df, df_add_pool, remove_pid, add_pid)
    print(f"[WHATIF] before_n={len(df)} after_n={len(df_after)}")

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

        before_net = compute_expected_net60_model_map(
            df,
            weighting,
            season_t,
            TRANS_F,
            TRANS_D,
            MODEL_MAP_F,
            MODEL_MAP_D,
            CENTER_NET60_F,
            CENTER_NET60_D,
        )
        after_net = compute_expected_net60_model_map(
            df_after,
            weighting,
            season_t,
            TRANS_F,
            TRANS_D,
            MODEL_MAP_F,
            MODEL_MAP_D,
            CENTER_NET60_F,
            CENTER_NET60_D,
        )

        role_label = f"Projected → next season ({season_next(season_t)})"

    else:
        comp_before = compute_composition(df, weighting)
        comp_after = compute_composition(df_after, weighting)

        if weighting == "toi":
            before_net = weighted_mean(df["es_net60"], df["toi_es_sec"])
            after_net = weighted_mean(df_after["es_net60"], df_after["toi_es_sec"])
        else:
            before_net = float(pd.to_numeric(df["es_net60"], errors="coerce").mean())
            after_net = float(pd.to_numeric(df_after["es_net60"], errors="coerce").mean())

        role_label = f"Current season ({season_t})"

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
        add_opts,
        delta.to_dict("records"),
        kpis,
    )


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050, use_reloader=False)
