from __future__ import annotations

import dash
from dash import html

# This is the Dash app object
app = dash.Dash(
    __name__,
    use_pages=True,  # if you use pages/
    suppress_callback_exceptions=True,
)

# Simple fallback layout (so it always boots even if pages are empty)
app.layout = html.Div(
    [
        html.H2("Cost Cup Dash"),
        dash.page_container,
    ]
)

# This is what gunicorn serves
server = app.server
