import os

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, dash_table, dcc, html
from sqlalchemy import text

from db_utils import get_db_engine  # your repo helper

dash.register_page(__name__, path="/tab-1", name="Tab 1 — Archetype Lookup")

PSQL_DSN = os.getenv("PSQL_DSN")  # optional if your get_db_engine uses .env already


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
  SELECT
    season,
    team_code,
    pos_group,
    cluster,
    toi_es_sec
  FROM mart.v_player_season_archetypes_modern_regulars
  WHERE season = :season
    AND team_code = :team_code
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


def season_label(season: int) -> str:
    s = int(season)
    y1 = s // 10000
    y2 = s % 10000
    return f"{y1}-{str(y2)[-2:]}"  # 20232024 -> "2023-24"


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_db_engine()
    try:
        return pd.read_sql_query(text(sql), engine, params=params or {})
    finally:
        engine.dispose()


def make_pct_bar(df_comp: pd.DataFrame, title: str):
    # expects pos_group, cluster, pct_pos_toi
    if df_comp.empty:
        return px.bar(title=title)

    # cluster -> string for nicer legend ordering
    df = df_comp.copy()
    df["cluster"] = df["cluster"].astype(int).astype(str)

    # stacked bars: one bar per pos_group (F/D) showing cluster %
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


# load dropdown options once at startup
df_seasons = read_df(SQL_SEASONS)
df_teams = read_df(SQL_TEAMS)

season_options = [
    {"label": season_label(s), "value": int(s)} for s in df_seasons["season"].tolist()
]

team_options = [{"label": f"{t}", "value": t} for t in df_teams["team_code"].tolist()]

default_season = season_options[-1]["value"] if season_options else 20242025
default_team = team_options[0]["value"] if team_options else None  # harmless fallback


layout = html.Div(
    style={"maxWidth": "1100px", "margin": "0 auto", "padding": "18px"},
    children=[
        html.H2("Team Archetype Composition (ES TOI-weighted)"),
        html.Div(
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
            ],
        ),
        html.Hr(),
        dcc.Graph(id="composition_graph"),
        html.H3("Composition table (within F/D, % of ES TOI)"),
        dash_table.DataTable(
            id="composition_table",
            columns=[
                {"name": "pos_group", "id": "pos_group"},
                {"name": "cluster", "id": "cluster"},
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
                {"name": "cluster", "id": "cluster"},
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

    fig = make_pct_bar(df_comp, title=f"{team_code} {season} — Archetype mix (% ES TOI within F/D)")

    # DataTables want plain dict records
    comp_records = df_comp.to_dict("records")
    top_records = df_top.to_dict("records")

    return fig, comp_records, top_records
