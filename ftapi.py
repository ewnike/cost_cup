"""
Created on February 27, 2025.

Python code for using FT's Valuation API.

@author: Eric Winiecke
"""

import json
import pprint

import requests

API_BASE = "https://valuation.api.ftoptions.aws.cboe.com/"
API_KEY = "97AB31E1-5128-4225-9302-C60EEFE10B9"

DEFAULT_HEADER = {"api_key": API_KEY}

# # Endpoint definitions
# ENDPOINT_EQUITY_OPTION_CLASS_FITTED_VOLATILITY = "/EquityOptionClassFittedVolatility/Get"
# ENDPOINT_EQUITY_OPTION_FIT_PARAMETERS_ARCHIVE =
# ENDPOINT_EQUITY_OPTION_HEDGE_PRICE_WHEN_DELTA =
# ENDPOINT_EQUITY_OPTION_PRICE_BY_CHAIN =
# ENDPOINT_EQUITY_OPTION_USER_FITTED_VOLATILITY =
# ENDPOINT_EQUITY_OPTION_VALUATION_PARAMETERS =
# ENDPOINT_FTREF_CALC_THEO =
# ENDPOINT_FTREF_CPSKEW_RANKING =
# ENDPOINT_FTREF_EARLY_EXERCISE_STRIKE_BY_OPTION_CLASS =
# ENDPOINT_FTREF_EARN_METRIC_HIST =
# ENDPOINT_FTREF_ETF_VOL_RATIOS =
# ENDPOINT_FTREF_EVENT_FREE_REALIZED_VOLATILITY =
# ENDPOINT_FTREF_FIT_PARAMETERS =
# ENDPOINT_FTREF_FIT_PARAMETERS_NS =
# ENDPOINT_FTREF_IMPLIED_BORROW =
# ENDPOINT_FTREF_IMPLIED_EARNINGS_MOVE =
# ENDPOINT_FTREF_MASTERVOL_BY_UNDERLYER =
# ENDPOINT_FTREF_MASTERVOL_BY_UNDERLYER_NS =
# ENDPOINT_FTREF_TRADED_VEGA_RATIO =
# ENDPOINT_STATUS =

# def get_data(url):
#     response = requests.get(url, headers=DEFAULT_HEADER)
#     return json.loads(response.text)

# def post_data(url, payload):
#     response = requests.post(url, headers=DEFAULT_HEADER, data=payload)
#     print(response.status_code)
#     return json.loads(response.text)


api_url = (
    "https://valuation.api.ftoptions.aws.cboe.com/GET /FTRefMasterVolByUnderlyer/GetAvailableFields"
)

headers = {
    api_key: API_KEY,
}

r = requests.post(api_url)
X = r.json()
pprint.pprint(X)
