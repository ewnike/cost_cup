from __future__ import annotations

from functools import lru_cache

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, State, callback_context, dash_table, dcc, html
from dash.dash_table.Format import Format, Scheme
from sqlalchemy import text

from db_utils import get_db_engine

dash.register_page(__name__, path="/tab-2", name="Tab 2 — Player Gamelog")

TAB2_GLOSSARY = [
    {
        "term": "es_net60",
        "definition": "Even-strength net attempts per 60 = cf60 - ca60 (5v5). Positive means your team out-attempted opponents with the player on ice.",
    },
    {
        "term": "rolling window (N)",
        "definition": "Rolling mean over the last N games INCLUDING the current game (historical smoothing). For prediction, you’d shift by 1 to exclude the current game.",
    },
    {"term": "cf60", "definition": "Corsi For per 60 at 5v5 while player is on ice."},
    {
        "term": "ca60",
        "definition": "Corsi Against per 60 at 5v5 while player is on ice.",
    },
    {"term": "cf_percent", "definition": "Corsi For % at 5v5 = cf / (cf + ca)."},
]

# ---------- SQL ----------
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

SQL_PLAYERS_BY_TEAM_SEASON = """
SELECT DISTINCT
  a.player_id
FROM mart.v_player_season_archetypes_modern_regulars a
WHERE a.season = :season
  AND a.team_code = :team_code
ORDER BY a.player_id;
"""

# Note: we use the season-specific truth table for speed and correctness.
# We join team_code for filtering/display.
SQL_PLAYER_GAMELOG = """
WITH game_calendar AS (
  SELECT
    season,
    game_id,
    MAX(game_date)::date AS game_date
  FROM raw.raw_shifts_resolved_skaters
  WHERE session = 'R'
  GROUP BY 1,2
)
SELECT
  f.season,
  f.game_id,
  gc.game_date,
  f.player_id,
  f.team_id,
  tc.team_code,

  f.toi_es_sec,
  ROUND(f.toi_es_sec / 60.0, 2) AS toi_es_min,

  f.cf,
  f.ca,
  f.cf60,
  f.ca60,
  f.cf_percent,

  (f.cf60 - f.ca60) AS es_net60,

  f.goals,
  f.assists,
  f.points,
  f.shots,
  f.hits,
  f.blocked,
  f.takeaways,
  f.giveaways,
  f.faceoff_wins,
  f.faceoff_taken,
  f.penalties_taken

FROM mart.player_game_features_20242025_truth f
LEFT JOIN dim.dim_team_code tc
  ON tc.team_id = f.team_id
LEFT JOIN game_calendar gc
  ON gc.season = f.season
 AND gc.game_id = f.game_id

WHERE f.player_id = :player_id
  AND f.season = :season
  AND tc.team_code = :team_code

ORDER BY gc.game_date NULLS LAST, f.game_id;
"""


# ---------- helpers ----------
@lru_cache(maxsize=1)
def _engine():
    return get_db_engine()


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql_query(text(sql), _engine(), params=params or {})


@lru_cache(maxsize=1)
def _seasons_df() -> pd.DataFrame:
    return read_df(SQL_SEASONS)


def _team_options_for_season(season: int):
    df_teams = read_df(SQL_TEAMS_BY_SEASON, {"season": int(season)})
    team_vals = df_teams["team_code"].tolist()
    options = [{"label": t, "value": t} for t in team_vals]
    default = team_vals[0] if team_vals else None
    return options, default


def _player_options_for_team_season(season: int, team_code: str | None):
    if not team_code:
        return [], None
    df_players = read_df(
        SQL_PLAYERS_BY_TEAM_SEASON, {"season": int(season), "team_code": str(team_code)}
    )
    pids = [int(x) for x in df_players["player_id"].tolist()]
    options = [{"label": str(pid), "value": pid} for pid in pids]
    default = pids[0] if pids else None
    return options, default


def season_label(season: int) -> str:
    s = int(season)
    y1 = s // 10000
    y2 = s % 10000
    return f"{y1}-{str(y2)[-2:]}"


def season_next(season: int) -> int:
    return int(season) + 10001


def season_truth_table(season: int) -> str:
    # guard: only allow seasons you expect; keeps SQL injection out
    allowed = {20182019, 20192020, 20202021, 20212022, 20222023, 20232024, 20242025}
    if season not in allowed:
        raise ValueError(f"Unsupported season: {season}")
    return f"mart.player_game_features_{season}_truth"


def load_gamelog(season: int, team_code: str, player_id: int) -> pd.DataFrame:
    tbl = season_truth_table(season)
    sql = SQL_PLAYER_GAMELOG.replace("mart.player_game_features_20242025_truth", tbl)
    return read_df(sql, params={"season": season, "team_code": team_code, "player_id": player_id})


def add_centered_rolling(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    # centered rolling window (uses past+future around each point)
    out["es_net60_roll"] = out["es_net60"].rolling(window=window, min_periods=1, center=True).mean()
    out["points_roll"] = out["points"].rolling(window=window, min_periods=1, center=True).mean()
    out["shots_roll"] = out["shots"].rolling(window=window, min_periods=1, center=True).mean()
    return out


# ---------- app ----------


# Clean UX: start with no team/player selected.
team_options0: list[dict] = []
player_options0: list[dict] = []
default_team = None
default_player = None


def layout():
    try:
        # ... you must define these before using them:
        # season_options, default_season, team_options0, player_options0
        # (from DB reads)

        return html.Div(
            style={"maxWidth": "1200px", "margin": "0 auto", "padding": "18px"},
            children=[
                # --- controls row ---
                html.Div(
                    style={
                        "display": "flex",
                        "gap": "12px",
                        "alignItems": "center",
                        "flexWrap": "wrap",
                        "marginTop": "10px",
                    },
                    children=[
                        html.Div(
                            style={"minWidth": "180px"},
                            children=[
                                html.Label("Season"),
                                dcc.Dropdown(
                                    id="tab2-season",
                                    options=season_options,
                                    value=default_season,
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "180px"},
                            children=[
                                html.Label("Team"),
                                dcc.Dropdown(
                                    id="tab2-team_code",
                                    options=team_options0,
                                    value=None,
                                    clearable=True,
                                    placeholder="Select a team...",
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "220px"},
                            children=[
                                html.Label("Player ID"),
                                dcc.Dropdown(
                                    id="tab2-player_id",
                                    options=player_options0,
                                    value=None,
                                    clearable=True,
                                    placeholder="Select a player...",
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "160px"},
                            children=[
                                html.Label("Rolling window (games)"),
                                dcc.Dropdown(
                                    id="roll_window",
                                    options=[{"label": str(x), "value": x} for x in [3, 5, 10, 15]],
                                    value=5,
                                    clearable=False,
                                ),
                            ],
                        ),
                        html.Div(
                            style={"minWidth": "260px"},
                            children=[
                                html.Label("Focus game # (center of window)"),
                                dcc.Slider(
                                    id="focus_game_n",
                                    min=1,
                                    max=82,  # updated dynamically in callback
                                    step=1,
                                    value=1,
                                    tooltip={"placement": "bottom", "always_visible": False},
                                ),
                            ],
                        ),
                    ],
                ),
                html.Hr(),
                # --- charts row ---
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "18px"},
                    children=[
                        html.Div(
                            children=[
                                html.H4(
                                    id="net_title",
                                    style={"margin": "0 0 2px 0", "lineHeight": "1.1"},
                                ),
                                dcc.Graph(id="net_graph"),
                            ]
                        ),
                        html.Div(
                            children=[
                                html.H4(
                                    id="ps_title",
                                    style={"margin": "0 0 2px 0", "lineHeight": "1.1"},
                                ),
                                dcc.Graph(id="ps_graph"),
                            ]
                        ),
                    ],
                ),
                html.H3("Game-by-game table"),
                dash_table.DataTable(
                    id="gamelog_table",
                    columns=[
                        {"name": "game_date", "id": "game_date"},
                        {"name": "game_id", "id": "game_id"},
                        {"name": "toi_es_min", "id": "toi_es_min"},
                        {"name": "cf", "id": "cf"},
                        {"name": "ca", "id": "ca"},
                        {
                            "name": "cf60",
                            "id": "cf60",
                            "type": "numeric",
                            "format": Format(precision=4, scheme=Scheme.fixed),
                        },
                        {
                            "name": "ca60",
                            "id": "ca60",
                            "type": "numeric",
                            "format": Format(precision=4, scheme=Scheme.fixed),
                        },
                        {
                            "name": "cf_percent",
                            "id": "cf_percent",
                            "type": "numeric",
                            "format": Format(precision=4, scheme=Scheme.fixed),
                        },
                        {
                            "name": "es_net60",
                            "id": "es_net60",
                            "type": "numeric",
                            "format": Format(precision=4, scheme=Scheme.fixed),
                        },
                        {"name": "goals", "id": "goals"},
                        {"name": "assists", "id": "assists"},
                        {"name": "points", "id": "points"},
                        {"name": "shots", "id": "shots"},
                        {"name": "hits", "id": "hits"},
                        {"name": "blocked", "id": "blocked"},
                        {"name": "faceoff_wins", "id": "faceoff_wins"},
                        {"name": "faceoff_taken", "id": "faceoff_taken"},
                    ],
                    page_size=20,
                    sort_action="native",
                    style_table={"overflowX": "auto"},
                    style_cell={"fontFamily": "Arial", "fontSize": 12, "padding": "6px"},
                ),
            ],
        )

    except Exception as e:
        return html.Div(
            style={"padding": "18px"},
            children=[
                html.H3("Tab 2 failed to load"),
                html.Pre(str(e)),
            ],
        )


@dash.callback(
    Output("tab2-glossary_modal", "style"),
    Input("tab2-open_glossary", "n_clicks"),
    Input("tab2-close_glossary", "n_clicks"),
    State("tab2-glossary_modal", "style"),
)
def toggle_modal(open_n, close_n, style):
    ctx = callback_context
    if not ctx.triggered:
        return style
    trig = ctx.triggered[0]["prop_id"].split(".")[0]

    new_style = dict(style or {})
    if trig == "tab2-open_glossary":
        new_style["display"] = "block"
    elif trig == "tab2-close_glossary":
        new_style["display"] = "none"
    return new_style


@dash.callback(
    Output("tab2-glossary_table", "data"),
    Input("tab2-glossary_search", "value"),
)
def filter_glossary(q):
    if not q:
        return TAB2_GLOSSARY
    return [r for r in TAB2_GLOSSARY if q in r["term"].lower() or q in r["definition"].lower()]


@dash.callback(
    Output("tab2-player_id", "options"),
    Output("tab2-player_id", "value"),
    Input("tab2-season", "value"),
    Input("tab2-team_code", "value"),
    State("tab2-player_id", "value"),
)
def refresh_players(season: int, team_code: str, player_id):
    if season is None or team_code is None:
        return [], None

    df = read_df(
        SQL_PLAYERS_BY_TEAM_SEASON,
        params={"season": int(season), "team_code": str(team_code)},
    )

    opts = [{"label": str(int(pid)), "value": int(pid)} for pid in df["player_id"].tolist()]
    valid = {o["value"] for o in opts}

    pid = int(player_id) if player_id is not None else None
    val = pid if pid in valid else None  # <-- this is the reset behavior

    return opts, val


@dash.callback(
    Output("net_title", "children"),
    Output("net_graph", "figure"),
    Output("ps_title", "children"),
    Output("ps_graph", "figure"),
    Output("gamelog_table", "data"),
    Output("focus_game_n", "max"),
    Output("focus_game_n", "value"),
    Input("tab2-season", "value"),
    Input("tab2-team_code", "value"),
    Input("tab2-player_id", "value"),
    Input("roll_window", "value"),
    Input("focus_game_n", "value"),
)
def refresh_gamelog(
    season: int, team_code: str, player_id: int, roll_window: int, focus_game_n: int
):
    # must return 7 outputs in ALL cases
    empty_fig = px.line()
    empty_fig.update_layout(title=None)

    if season is None or team_code is None or player_id is None:
        return "", empty_fig, "", empty_fig, [], 1, 1

    df = load_gamelog(int(season), str(team_code), int(player_id))
    if df.empty:
        return (
            "No rows for selection",
            empty_fig,
            "No rows for selection",
            empty_fig,
            [],
            1,
            1,
        )

    sort_cols = [c for c in ["game_date", "game_id"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    # x axis = game number (1..N) to avoid 2.024e9 style labels
    df["game_n"] = range(1, len(df) + 1)

    max_game_n = int(df["game_n"].max())
    focus = int(focus_game_n or 1)
    focus = max(1, min(focus, max_game_n))

    # centered rolling
    df = add_centered_rolling(df, window=int(roll_window))

    # show only a window around the focus game (reduces clutter)
    half = int(roll_window)  # you can tune this
    lo = max(1, focus - half)
    hi = min(max_game_n, focus + half)
    df_plot = df[(df["game_n"] >= lo) & (df["game_n"] <= hi)].copy()

    title_net = (
        f"{team_code} {season} — player {player_id}: ES net60 per game (rolling={roll_window})"
    )
    title_ps = (
        f"{team_code} {season} — player {player_id}: points/shots per game (rolling={roll_window})"
    )

    df_net = df_plot[["game_n", "game_id", "game_date", "es_net60", "es_net60_roll"]].copy()
    df_net_melt = df_net.melt(
        id_vars=["game_n", "game_id", "game_date"],
        var_name="series",
        value_name="value",
    )
    fig_net = px.line(
        df_net_melt,
        x="game_n",
        y="value",
        color="series",
        markers=True,
        hover_data={"game_id": True, "game_n": True, "game_date": True},
    )
    fig_net.update_layout(
        title=None,
        xaxis_title="Game #",
        yaxis_title="ES net60",
        margin=dict(l=40, r=20, t=10, b=40),
    )
    n = len(df_plot)
    tick = 1 if n <= 15 else 2 if n <= 35 else 5 if n <= 70 else 10
    fig_net.update_xaxes(dtick=tick)

    df_ps = df_plot[
        [
            "game_n",
            "game_id",
            "game_date",
            "points",
            "points_roll",
            "shots",
            "shots_roll",
        ]
    ].copy()
    df_ps_melt = df_ps.melt(
        id_vars=["game_n", "game_id", "game_date"],
        var_name="series",
        value_name="value",
    )
    fig_ps = px.line(
        df_ps_melt,
        x="game_n",
        y="value",
        color="series",
        markers=True,
        hover_data={"game_id": True, "game_n": True, "game_date": True},
    )
    # table records: also use df_plot so it matches what you see
    records = df_plot.to_dict("records")

    fig_ps.update_layout(
        title=None,
        xaxis_title="Game #",
        yaxis_title="count",
        margin=dict(l=40, r=20, t=10, b=40),
    )
    fig_ps.update_xaxes(dtick=tick)

    return title_net, fig_net, title_ps, fig_ps, records, max_game_n, focus
