from __future__ import annotations

import os
import dash
from dash import html

app = dash.Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
)

NAV_STYLE = {
    "display": "flex",
    "gap": "10px",
    "alignItems": "center",
    "flexWrap": "wrap",
}


def nav_button(label: str, href: str) -> html.A:
    return html.A(
        label,
        href=href,
        style={
            "padding": "10px 14px",
            "border": "1px solid #ddd",
            "borderRadius": "10px",
            "textDecoration": "none",
            "display": "inline-block",
            "background": "white",
        },
    )


app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Cost Cup Dash", style={"margin": "0"}),
                        html.Div(
                            [
                                nav_button("Home", "/"),
                                nav_button("Tab 1 — Archetype Lookup", "/tab-1"),
                                nav_button("Tab 2 — Player Gamelog", "/tab-2"),
                                nav_button("Tab 3 — Team What-If", "/tab-3"),
                            ],
                            style=NAV_STYLE,
                        ),
                    ],
                    style={"display": "grid", "gap": "12px"},
                ),
            ],
            style={"padding": "16px"},
        ),
        html.Hr(style={"margin": "0"}),
        html.Div(dash.page_container, style={"padding": "16px"}),
    ]
)

server = app.server

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8050"))
    app.run(host="0.0.0.0", port=port, debug=True)
