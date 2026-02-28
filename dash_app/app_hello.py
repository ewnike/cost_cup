from dash import Dash, html

app = Dash(__name__)
server = app.server  # useful later for deployment

app.layout = html.Div(
    [
        html.H2("Cost Cup Dash"),
        html.Div("Dash is running."),
    ],
    style={"padding": "20px"},
)

if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)
