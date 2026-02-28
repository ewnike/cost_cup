import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, dash_table, dcc, html
from dash.dash_table.Format import Format, Scheme, Sign
from sqlalchemy import text

from db_utils import get_db_engine  # same import style as your other working pages

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


# ---------------- helpers ----------------
def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_db_engine()
    try:
        return pd.read_sql_query(text(sql), engine, params=params or {})
    finally:
        engine.dispose()


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


# def weighted_mean(series, weights):
#     w = weights.astype(float)
#     s = series.astype(float)
#     denom = w.sum()
#     return float((s * w).sum() / denom) if denom else 0.0


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    m = v.notna() & w.notna() & (w > 0)
    if not m.any():
        return 0.0
    return float((v[m] * w[m]).sum() / w[m].sum())


# def apply_whatif(df: pd.DataFrame, remove_pid: int | None, add_pid: int | None) -> pd.DataFrame:
#     out = df.copy()

#     if remove_pid is not None:
#         out = out[out["player_id"] != remove_pid].copy()

#     # V1 safety: only allow adding a player that exists in the current roster df
#     if add_pid is not None and add_pid in set(df["player_id"]):
#         out = pd.concat([out, df[df["player_id"] == add_pid].copy()], ignore_index=True)

#     return out


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


def apply_whatif(
    df_roster: pd.DataFrame,
    df_add_pool: pd.DataFrame,
    remove_pid: int | None,
    add_pid: int | None,
) -> pd.DataFrame:
    out = df_roster.copy()

    if remove_pid is not None:
        out = out[out["player_id"] != remove_pid].copy()

    if add_pid is not None:
        cand = df_add_pool[df_add_pool["player_id"] == add_pid]
        if not cand.empty:
            cand = cand.iloc[[0]].copy()  # keep 1 row
            cand = cand.reindex(columns=out.columns)  # align schema, fill missing with NaN
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
            ],
        ),
        html.Hr(),
        dcc.Graph(id="comp_graph"),
        # KPI row (callback will populate children)
        html.Div(
            id="kpi_row",
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginTop": "8px"},
        ),
        html.Div(style={"height": "12px"}),
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
                        # keep your delta table below this (whatif_table)
                        #     ]
                        # )
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
    Output("comp_graph", "figure"),
    Output("roster_table", "data"),
    Output("remove_player", "options"),
    Output("add_player", "options"),
    Output("whatif_table", "data"),
    Output("kpi_row", "children"),
    Input("season", "value"),
    Input("team_code", "value"),
    Input("weighting", "value"),
    Input("remove_player", "value"),
    Input("add_player", "value"),
)
def refresh(season, team_code, weighting, remove_pid, add_pid):
    empty_fig = px.bar(title="")
    if season is None or team_code is None:
        return empty_fig, [], [], [], [], []

    df = read_df(SQL_TEAM_ROSTER_ARCH, {"season": int(season), "team_code": str(team_code)})
    if df.empty:
        return px.bar(title="No roster rows"), [], [], [], [], []

    # roster table
    roster = df.copy()
    roster["toi_es_min"] = (roster["toi_es_sec"] / 60.0).round(1)
    roster["toi_pg"] = roster["toi_per_game"].astype(float).round(2)
    roster["es_net60"] = roster["es_net60"].astype(float).round(2)
    roster_records = roster[
        ["pos_group", "cluster", "player_id", "toi_es_min", "toi_pg", "es_net60"]
    ].to_dict("records")

    # remove options = team roster
    roster_pids = sorted(roster["player_id"].unique().tolist())
    remove_opts = [{"label": str(pid), "value": int(pid)} for pid in roster_pids]

    # add pool = league regulars (excluding roster)
    df_add_pool = read_df(
        SQL_ADD_CANDIDATES,
        {"season": int(season), "exclude_pids": roster_pids},
    )

    # if removing, restrict add list to same pos_group
    pos_filter = None
    if remove_pid is not None and remove_pid in set(roster["player_id"]):
        pos_filter = roster.loc[roster["player_id"] == remove_pid, "pos_group"].iloc[0]

    add_pool_f = df_add_pool
    if pos_filter is not None:
        add_pool_f = add_pool_f[add_pool_f["pos_group"] == pos_filter].copy()

    # add options show helpful label
    add_opts = [
        {
            "label": f"{int(r.player_id)} ({r.team_code}) {r.pos_group} c{int(r.cluster)} toi_es_min={round(r.toi_es_sec / 60.0, 1)}",
            "value": int(r.player_id),
        }
        for r in add_pool_f.itertuples(index=False)
    ][:400]  # cap list size for UI sanity

    # composition before
    comp_before = compute_composition(df, weighting)
    fig = make_comp_fig(
        comp_before,
        title=f"{team_code} {season} — archetype mix ({'TOI' if weighting == 'toi' else 'counts'})",
    )

    # composition after
    df_after = apply_whatif(df, df_add_pool, remove_pid, add_pid)
    if weighting == "toi":
        before_net = weighted_mean(df["es_net60"], df["toi_es_sec"])
        after_net = weighted_mean(df_after["es_net60"], df_after["toi_es_sec"])
    else:
        before_net = float(pd.to_numeric(df["es_net60"], errors="coerce").mean())
        after_net = float(pd.to_numeric(df_after["es_net60"], errors="coerce").mean())
    delta_net = round(after_net - before_net, 3)

    comp_after = compute_composition(df_after, weighting)

    key = ["pos_group", "cluster"]
    wb = comp_before[key + ["pct"]].rename(columns={"pct": "pct_before"})
    wa = comp_after[key + ["pct"]].rename(columns={"pct": "pct_after"})
    delta = wb.merge(wa, on=key, how="outer").fillna(0)
    delta["delta_pct"] = (delta["pct_after"] - delta["pct_before"]).round(2)
    delta = delta.sort_values(["pos_group", "cluster"])

    kpis = [
        kpi_box("ES net60 (before)", before_net),
        kpi_box("ES net60 (after)", after_net),
        kpi_box(
            "Δ ES net60",
            delta_net,
            color=("#2ca02c" if delta_net > 0 else "#d62728" if delta_net < 0 else None),
        ),
    ]

    return fig, roster_records, remove_opts, add_opts, delta.to_dict("records"), kpis


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)
