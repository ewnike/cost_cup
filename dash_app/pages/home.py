from __future__ import annotations

import dash
from dash import dcc, html

dash.register_page(__name__, path="/", name="Home", order=0)

CARD_STYLE = {
    "border": "1px solid #ddd",
    "borderRadius": "14px",
    "padding": "14px",
    "background": "white",
    "boxShadow": "0 1px 6px rgba(0,0,0,0.04)",
}


def layout():
    return html.Div(
        children=[
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr 1fr",
                    "gap": "14px",
                },
                children=[
                    html.Div(
                        style=CARD_STYLE,
                        children=[
                            html.H3("Where to start"),
                            html.Ul(
                                children=[
                                    html.Li(
                                        [
                                            dcc.Link(
                                                "Tab 1 — Archetype Lookup",
                                                href="/tab-1",
                                            ),
                                            ": Look up a player/season and see their archetype cluster + context.",
                                        ]
                                    ),
                                    html.Li(
                                        [
                                            dcc.Link("Tab 2 — Player Gamelog", href="/tab-2"),
                                            ": Time-series of player game-level stats (trend / volatility / context).",
                                        ]
                                    ),
                                    html.Li(
                                        [
                                            dcc.Link("Tab 3 — Team What-If", href="/tab-3"),
                                            ": Team composition breakdown + projected next-season mix + roster what-if.",
                                        ]
                                    ),
                                ]
                            ),
                        ],
                    ),
                    html.Div(
                        style=CARD_STYLE,
                        children=[
                            html.H3("Core idea"),
                            html.P(
                                [
                                    "We reduce player seasons into ",
                                    html.B("3 archetype clusters per position group (F/D)"),
                                    " using unsupervised learning (KMeans). Then we model how players move between clusters season-to-season using a Markov transition model. ",
                                    "We also train a supervised multinomial logistic regression to estimate player-specific transition probabilities when available. ",
                                    "This gives us a data-driven way to forecast how a team’s mix of roles—and expected performance—may shift next season, including the impact of adds/removals (trades, free agency, injuries).",
                                ]
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(style={"height": "14px"}),
            html.Div(
                style=CARD_STYLE,
                children=[
                    html.H3("How to interpret the models"),
                    html.H4("What is a cluster?"),
                    html.P(
                        [
                            "A cluster is an archetype group learned from season-level features (rates, usage, possession, etc.). ",
                            "KMeans assigns each player-season to the nearest cluster center, producing labels {0,1,2} separately for forwards (F) and defense (D).",
                        ]
                    ),
                    html.H4("What does “Projected” mean (Tab 3)?"),
                    html.P(
                        [
                            "“Projected” means we’re estimating the team’s ",
                            html.B("next-season"),
                            " archetype mix using transition probabilities from season_t → season_t+1. ",
                            "So the bars represent expected % of F/D in clusters 0/1/2 next year, not this year’s realized clusters.",
                        ]
                    ),
                    html.H4("What does the KPI represent?"),
                    html.P(
                        [
                            "The KPI is team-level ES net60 aggregated across roster players. In projected mode we replace each player’s single cluster label with an ",
                            html.B("expected value over next-season clusters"),
                            " using probabilities p(to_cluster=k).",
                        ]
                    ),
                    html.H4("Fallback behavior (important)"),
                    html.P(
                        [
                            "When player-specific supervised probabilities exist (mart.cluster_transition_model_probs_f/d), we use those. ",
                            "If not available for that (season_t, player_id), we fall back to the position-group transition matrix ",
                            "(Dirichlet-smoothed Markov transition probabilities).",
                        ]
                    ),
                ],
            ),
            html.Div(style={"height": "14px"}),
            html.Div(
                style=CARD_STYLE,
                children=[
                    html.H3("Dirichlet smoothing + Markov transitions (what we did)"),
                    html.P(
                        [
                            "We estimate a 3×3 transition matrix per position group where each row is: ",
                            html.Code("P(cluster_{t+1}=j | cluster_t=i)"),
                            ". To avoid zero probabilities (especially with small samples), we apply Dirichlet smoothing to each row. ",
                            "This acts like adding pseudo-counts to the transition counts so every row is well-formed and sums to 1.",
                        ]
                    ),
                    html.P("DB tables used for the smoothed transition matrices:"),
                    html.Ul(
                        children=[
                            html.Li(html.Code("mart.cluster_transitions_modern_f")),
                            html.Li(html.Code("mart.cluster_transitions_modern_d")),
                        ]
                    ),
                    html.P(
                        [
                            "If you want a quick background reference: ",
                            html.A(
                                "Dirichlet distribution (Wikipedia)",
                                href="https://en.wikipedia.org/wiki/Dirichlet_distribution",
                                target="_blank",
                            ),
                        ]
                    ),
                ],
            ),
            html.Div(style={"height": "14px"}),
            html.Div(
                style=CARD_STYLE,
                children=[
                    html.H3("Supervised transition model (logistic regression)"),
                    html.P(
                        [
                            "We also fit a multinomial logistic regression separately for F and D that predicts cluster_{t+1} from season_t features. ",
                            "This produces player-specific probabilities p_to0/p_to1/p_to2 stored in:",
                        ]
                    ),
                    html.Ul(
                        children=[
                            html.Li(html.Code("mart.cluster_transition_model_probs_f")),
                            html.Li(html.Code("mart.cluster_transition_model_probs_d")),
                        ]
                    ),
                ],
            ),
        ]
    )
