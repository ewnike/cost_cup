from __future__ import annotations

import dash
from dash import dcc, html

app = dash.Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
)


def navbar() -> html.Div:
    # Build buttons from registered pages
    links = []
    for page in dash.page_registry.values():
        # Optional: control ordering with page.get("order", 999)
        links.append(
            dcc.Link(
                page["name"],
                href=page["path"],
                style={
                    "padding": "8px 12px",
                    "border": "1px solid #bbb",
                    "borderRadius": "10px",
                    "textDecoration": "none",
                    "display": "inline-block",
                },
            )
        )

    return html.Div(
        links,
        style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginBottom": "12px"},
    )


app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "18px"},
    children=[
        html.H2("Cost Cup Dash"),
        navbar(),
        dash.page_container,
    ],
)

server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
