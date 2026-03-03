from __future__ import annotations

import os

import dash
from dash import html

# Create Dash app FIRST (required for dash.register_page to work)
app = dash.Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
)

app.layout = html.Div(
    [
        html.H2("Cost Cup Dash"),
        dash.page_container,
    ]
)

# What gunicorn serves
server = app.server

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8050"))
    # Dash >=2.15 prefers app.run
    app.run(host="0.0.0.0", port=port, debug=True)
