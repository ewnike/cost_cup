from __future__ import annotations

import os

import dash
import dash_auth
from dash import dcc, html

# --- App ---
app = dash.Dash(__name__, use_pages=True, suppress_callback_exceptions=True)

# --- Optional Basic Auth (safe for EB) ---
VALID_USERNAME = os.environ.get("APP_USER", "prof")
VALID_PASSWORD = os.environ.get("APP_PASS", "changeme")
dash_auth.BasicAuth(app, {VALID_USERNAME: VALID_PASSWORD})


# --- Navbar ---
def navbar() -> html.Div:
    pages = sorted(
        dash.page_registry.values(),
        key=lambda p: (p.get("order", 999), p.get("name", "")),
    )

    links = [
        dcc.Link(
            p["name"],
            href=p["path"],
            style={
                "padding": "8px 12px",
                "border": "1px solid #bbb",
                "borderRadius": "10px",
                "textDecoration": "none",
                "display": "inline-block",
            },
        )
        for p in pages
    ]

    return html.Div(
        links,
        style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginBottom": "12px"},
    )


# --- Layout ---
app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "18px"},
    children=[
        html.H2("Cost Cup Dash"),
        navbar(),
        dash.page_container,
    ],
)

# --- WSGI for Elastic Beanstalk / gunicorn ---
server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8050")), debug=True)
