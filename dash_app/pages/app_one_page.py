from functools import lru_cache

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, State, ctx, dash_table, dcc, html
from sqlalchemy import text

from dash_app.constants import CLUSTER_LABEL
from db_utils import get_db_engine

dash.register_page(__name__, path="/tab-1", name="Tab 1 — Archetype Lookup", order=1)

TAB1_GLOSSARY = [
    {
        "term": "Cluster (0/1/2)",
        "definition": (
            "A 3-bucket archetype label from the clustering model. "
            "0/1/2 are IDs (not ranks). Each cluster represents a different player role/style "
            "based on the features used in the model."
        ),
    },
    {
        "term": "Cluster labels (recommended)",
        "definition": (
            "Optional UI mapping to make clusters readable:\n"
            "0 = Defensive / low-event\n"
            "1 = Balanced / two-way\n"
            "2 = Offensive / high-event\n"
            "(Confirm these match your cluster profiles.)"
        ),
    },
    {
        "term": "Ranking / sorting",
        "definition": (
            "Composition table is grouped by pos_group and cluster. "
            "Top players table is sorted by even-strength TOI (toi_es_sec) descending "
            "so the biggest ES contributors appear first."
        ),
    },
    {
        "term": "% of ES TOI (within pos)",
        "definition": (
            "For each position group (F/D), percent of that group’s even-strength TOI "
            "that comes from each cluster. Values sum to 100% within each pos_group."
        ),
    },
]

# ---------------- SQL ----------------
SQL_SEASONS = """
SELECT DISTINCT season
FROM mart.v_player_season_archetypes_modern_regulars
ORDER BY season;
"""

SQL_TEAMS = """
SELECT DISTINCT team_code
FROM mart.v_player_season_archetypes_modern_regulars
ORDER BY team_code;
"""

SQL_COMPOSITION = """
WITH base AS (
  SELECT season, team_code, pos_group, cluster, toi_es_sec
  FROM mart.v_player_season_archetypes_modern_regulars
  WHERE season = :season AND team_code = :team_code
)
SELECT
  pos_group,
  cluster,
  SUM(toi_es_sec) AS toi_es_sec,
  ROUND(
    100.0 * SUM(toi_es_sec) /
    NULLIF(SUM(SUM(toi_es_sec)) OVER (PARTITION BY pos_group), 0),
    2
  ) AS pct_pos_toi
FROM base
GROUP BY 1,2
ORDER BY 1,2;
"""

SQL_TOP_PLAYERS = """
SELECT
  pos_group,
  cluster,
  player_id,
  team_code,
  ROUND(toi_es_sec/60.0, 1) AS toi_es_min,
  ROUND(toi_per_game::numeric, 2) AS toi_pg,
  ROUND((cf60 - ca60)::numeric, 2) AS es_net60,
  ROUND(cf60::numeric, 2) AS cf60,
  ROUND(ca60::numeric, 2) AS ca60,
  ROUND(cf_percent::numeric, 3) AS cf_pct
FROM mart.v_player_season_archetypes_modern_regulars
WHERE season = :season
  AND team_code = :team_code
ORDER BY toi_es_sec DESC
LIMIT 50;
"""


# ---------------- helpers ----------------


def add_cluster_label(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "cluster" not in df.columns:
        return df
    out = df.copy()
    out["cluster"] = pd.to_numeric(out["cluster"], errors="coerce").astype("Int64")
    out["cluster_label"] = out["cluster"].map(CLUSTER_LABEL).fillna(out["cluster"].astype(str))
    return out


@lru_cache(maxsize=1)
def _engine():
    return get_db_engine()


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql_query(text(sql), _engine(), params=params or {})


def season_label(season: int) -> str:
    s = int(season)
    y1 = s // 10000
    y2 = s % 10000
    return f"{y1}-{str(y2)[-2:]}"


def make_pct_bar(df_comp: pd.DataFrame, title: str):
    if df_comp.empty:
        return px.bar(title=title)
    df = df_comp.copy()
    df["cluster"] = df["cluster"].astype(int).astype(str)
    fig = px.bar(
        df,
        x="pos_group",
        y="pct_pos_toi",
        color="cluster",
        barmode="stack",
        text="pct_pos_toi",
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="inside")
    fig.update_layout(yaxis_title="% of ES TOI (within pos)")
    return fig


# ---------------- page layout ----------------
def layout():
    try:
        # DB calls happen here, not at import time
        df_seasons = read_df(SQL_SEASONS)
        season_vals = [int(s) for s in df_seasons["season"].tolist()]
        season_options = [{"label": season_label(s), "value": s} for s in season_vals]
        default_season = season_vals[-1] if season_vals else 20242025

        df_teams = read_df(SQL_TEAMS)
        team_vals = [str(t) for t in df_teams["team_code"].tolist()]
        team_options = [{"label": t, "value": t} for t in team_vals]
        default_team = team_vals[0] if team_vals else None

        controls_row = html.Div(
            style={"display": "flex", "gap": "12px", "alignItems": "center"},
            children=[
                html.Div(
                    style={"minWidth": "200px"},
                    children=[
                        html.Label("Season"),
                        dcc.Dropdown(
                            id="tab1-season",
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
                            id="tab1-team_code",
                            options=team_options,
                            value=default_team,
                            clearable=False,
                        ),
                    ],
                ),
                html.Button(
                    "Glossary / Definitions",
                    id="tab1-open_glossary",
                    n_clicks=0,
                    style={
                        "padding": "8px 12px",
                        "border": "1px solid #bbb",
                        "borderRadius": "10px",
                        "background": "#f8f8f8",
                        "cursor": "pointer",
                        "whiteSpace": "nowrap",
                    },
                ),
            ],
        )

        glossary_modal = html.Div(
            id="tab1-glossary_modal",
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
                        "padding": "16px",
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
                                    id="tab1-close_glossary",
                                    n_clicks=0,
                                ),
                            ],
                        ),
                        html.Hr(),
                        dcc.Input(
                            id="tab1-glossary_search",
                            type="text",
                            placeholder="Search definitions…",
                            style={"width": "360px", "padding": "6px"},
                        ),
                        html.Div(style={"height": "10px"}),
                        dash_table.DataTable(
                            id="tab1-glossary_table",
                            columns=[
                                {"name": "term", "id": "term"},
                                {"name": "definition", "id": "definition"},
                            ],
                            data=TAB1_GLOSSARY,
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
        )

        return html.Div(
            style={"maxWidth": "1100px", "margin": "0 auto", "padding": "18px"},
            children=[
                html.H2("Team Archetype Composition (ES TOI-weighted)"),
                controls_row,
                glossary_modal,
                html.Hr(),
                dcc.Graph(id="composition_graph"),
                html.H3("Composition table (within F/D, % of ES TOI)"),
                dash_table.DataTable(
                    id="composition_table",
                    columns=[
                        {"name": "pos_group", "id": "pos_group"},
                        {"name": "cluster", "id": "cluster_label"},
                        {"name": "toi_es_sec", "id": "toi_es_sec"},
                        {"name": "pct_pos_toi", "id": "pct_pos_toi"},
                    ],
                    page_size=20,
                    style_table={"overflowX": "auto"},
                    style_cell={"fontFamily": "Arial", "fontSize": 13, "padding": "6px"},
                ),
                html.Hr(),
                html.H3("Top players by ES TOI (context)"),
                dash_table.DataTable(
                    id="top_players_table",
                    columns=[
                        {"name": "pos_group", "id": "pos_group"},
                        {"name": "cluster", "id": "cluster_label"},
                        {"name": "player_id", "id": "player_id"},
                        {"name": "toi_es_min", "id": "toi_es_min"},
                        {"name": "toi_pg", "id": "toi_pg"},
                        {"name": "es_net60", "id": "es_net60"},
                        {"name": "cf60", "id": "cf60"},
                        {"name": "ca60", "id": "ca60"},
                        {"name": "cf_pct", "id": "cf_pct"},
                    ],
                    page_size=15,
                    sort_action="native",
                    style_table={"overflowX": "auto"},
                    style_cell={"fontFamily": "Arial", "fontSize": 13, "padding": "6px"},
                ),
            ],
        )

    except Exception as e:
        return html.Div(
            style={"maxWidth": "1100px", "margin": "0 auto", "padding": "18px"},
            children=[
                html.H2("Tab 1 — Archetype Lookup"),
                html.H3("Tab 1 failed to load"),
                html.P(
                    "The page hit an exception while loading dropdown options from the database."
                ),
                html.Pre(str(e), style={"whiteSpace": "pre-wrap", "color": "crimson"}),
            ],
        )


@dash.callback(
    Output("composition_graph", "figure"),
    Output("composition_table", "data"),
    Output("top_players_table", "data"),
    Input("tab1-season", "value"),
    Input("tab1-team_code", "value"),
)
def refresh_team_view(season: int, team_code: str):
    params = {"season": int(season), "team_code": str(team_code)}
    df_comp = read_df(SQL_COMPOSITION, params=params)
    df_top = read_df(SQL_TOP_PLAYERS, params=params)

    # add labels before returning table records
    df_comp = add_cluster_label(df_comp)
    df_top = add_cluster_label(df_top)

    fig = make_pct_bar(df_comp, title=f"{team_code} {season} — Archetype mix (% ES TOI within F/D)")
    return fig, df_comp.to_dict("records"), df_top.to_dict("records")


@dash.callback(
    Output("tab1-glossary_modal", "style"),
    Input("tab1-open_glossary", "n_clicks"),
    Input("tab1-close_glossary", "n_clicks"),
    State("tab1-glossary_modal", "style"),
)
def tab1_toggle_modal(open_n, close_n, style):
    if not ctx.triggered_id:
        return style
    new_style = dict(style or {})
    if ctx.triggered_id == "tab1-open_glossary":
        new_style["display"] = "block"
    elif ctx.triggered_id == "tab1-close_glossary":
        new_style["display"] = "none"
    return new_style


@dash.callback(Output("tab1-glossary_table", "data"), Input("tab1-glossary_search", "value"))
def tab1_filter_glossary(q):
    if not q:
        return TAB1_GLOSSARY
    q = q.lower()
    out = []
    for r in TAB1_GLOSSARY:
        term = str(r.get("term", "")).lower()
        definition = str(r.get("definition", "")).lower()
        if q in term or q in definition:
            out.append(r)
    return out
