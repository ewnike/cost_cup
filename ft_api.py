import requests
import json

api_url = "https://valuation.api.ftoptions.aws.cboe.com/EquityOptionPriceByChain/Get?underlyerSymbol=AAPL&api_key=97AB31E1-5128-4225-9301-C60EEFE10B99"

r = requests.get(api_url)

X = r.json()
print(X)
