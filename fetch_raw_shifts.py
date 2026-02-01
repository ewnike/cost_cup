import logging
import time

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(message)s")

SHIFT_URL = (
    "https://evolving-hockey.com/stats/shift_query/"
    "session/796c5201860b86f528c13dd965bf7858/dataobj/shift_q_table_output?w=&nonce=3a5908e20dbe7287"
)

COOKIE = (
    "__utmz=88564303.1759282999.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); "
    "wordpress_res=ewnike66%20b8b1db79468d9fe2e8382ee799b561abefeef792; "
    "cookie_notice_accepted=true; "
    "PHPSESSID=mq9s7eg53g3bn9ttns2bp4rtd0; "
    "pmpro_visit=1; "
    "pvc_visits[0]=1769995179b1042a1769995185b25; "
    "wordpress_test_cookie=WP%20Cookie%20check; "
    "wordpress_logged_in_76ede415765edb5e6771596370878b0f="
    "ewnike66%7C1801444787%7C0jaoOUO2EU5trOl8qQuk5ZKFtuzvhj8G9sf2K1v7OEM%7C"
    "a3e86d7e2ec04807e714bcd976fc79c951da72733f7856774e892485e330f881; "
    "wfwaf-authcookie-1050095bdac4729d84e83d3a7ae63a49="
    "6222%7Csubscriber%7Cread%7C27df10045048ee42f48529ac2e6a883f8589ca48615c85a94672b1b5e379961d; "
    "__utmc=88564303; "
    "__utma=88564303.1778612032.1759282999.1769908807.1769912264.37; "
    "__utmb=88564303.1.10.1769912264"
)

BASE_PAYLOAD_STR = "draw=1&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=+&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=player&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=team_num&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=position&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=game_id&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=5&columns%5B5%5D%5Bname%5D=game_date&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=6&columns%5B6%5D%5Bname%5D=season&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=7&columns%5B7%5D%5Bname%5D=session&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=8&columns%5B8%5D%5Bname%5D=team&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=9&columns%5B9%5D%5Bname%5D=opponent&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=10&columns%5B10%5D%5Bname%5D=is_home&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=true&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Bdata%5D=11&columns%5B11%5D%5Bname%5D=game_period&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=true&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Bdata%5D=12&columns%5B12%5D%5Bname%5D=shift_num&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=true&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Bdata%5D=13&columns%5B13%5D%5Bname%5D=seconds_start&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=true&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B14%5D%5Bdata%5D=14&columns%5B14%5D%5Bname%5D=seconds_end&columns%5B14%5D%5Bsearchable%5D=true&columns%5B14%5D%5Borderable%5D=true&columns%5B14%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B14%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B15%5D%5Bdata%5D=15&columns%5B15%5D%5Bname%5D=seconds_duration&columns%5B15%5D%5Bsearchable%5D=true&columns%5B15%5D%5Borderable%5D=true&columns%5B15%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B15%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B16%5D%5Bdata%5D=16&columns%5B16%5D%5Bname%5D=shift_start&columns%5B16%5D%5Bsearchable%5D=true&columns%5B16%5D%5Borderable%5D=true&columns%5B16%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B16%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B17%5D%5Bdata%5D=17&columns%5B17%5D%5Bname%5D=shift_end&columns%5B17%5D%5Bsearchable%5D=true&columns%5B17%5D%5Borderable%5D=true&columns%5B17%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B17%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B18%5D%5Bdata%5D=18&columns%5B18%5D%5Bname%5D=duration&columns%5B18%5D%5Bsearchable%5D=true&columns%5B18%5D%5Borderable%5D=true&columns%5B18%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B18%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B19%5D%5Bdata%5D=19&columns%5B19%5D%5Bname%5D=shift_mod&columns%5B19%5D%5Bsearchable%5D=true&columns%5B19%5D%5Borderable%5D=true&columns%5B19%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B19%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=50&search%5Bvalue%5D=&search%5Bregex%5D=false&search%5BcaseInsensitive%5D=true&search%5Bsmart%5D=true&escape=false"


def make_payload(start: int, page_size: int) -> str:
    """
    Use the original payload string, only changing start and length.
    Assumes BASE_PAYLOAD_STR contains 'start=0' and 'length=50'.
    """
    payload = BASE_PAYLOAD_STR.replace("start=0", f"start={start}")
    payload = payload.replace("length=50", f"length={page_size}")
    return payload


def fetch_all_shifts(page_size: int = 100) -> pd.DataFrame:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://evolving-hockey.com",
            "Referer": "https://evolving-hockey.com/stats/shift_query/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    session.headers["Cookie"] = COOKIE

    all_rows = []
    start = 0
    total = None

    while True:
        logging.info(f"Requesting rows starting at {start}…")
        payload = make_payload(start, page_size)

        resp = session.post(SHIFT_URL, data=payload, timeout=30)

        if resp.status_code == 404:
            logging.error("404 – session/nonce likely expired. Copy a fresh cURL from DevTools.")
            break

        resp.raise_for_status()
        js = resp.json()
        logging.info(f"JSON keys: {list(js.keys())}")

        if "error" in js:
            logging.error("Server error from Evolving-Hockey:")
            logging.error(js["error"])
            break

        if total is None:
            total = js.get("recordsTotal", 0)
            logging.info(
                f"recordsTotal: {total} recordsFiltered: {js.get('recordsFiltered', '??')}"
            )

        rows = js.get("data", [])
        logging.info(f"  got {len(rows)} rows")

        if not rows:
            break

        all_rows.extend(rows)
        start += page_size

        if start >= total:
            break

        time.sleep(0.2)

    logging.info(f"Total rows collected: {len(all_rows)}")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    logging.info(f"Final DataFrame rows: {len(df)}")
    return df


if __name__ == "__main__":
    df = fetch_all_shifts(page_size=100)

    out_name = "4_days_raw_shifts_20242025.csv"
    if df.empty:
        logging.warning(f"No data retrieved. NOT overwriting {out_name} with empty CSV.")
    else:
        df.to_csv(out_name, index=False)
        logging.info(f"Saved {out_name}")
