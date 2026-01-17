import os
import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BASE_URL = "https://www.spotrac.com/nhl/cap/_/year/{}/sort/cap_maximum_space2"
YEARS = [2018, 2019, 2020, 2021, 2022, 2023, 2024]

OUTPUT_DIR = "team_salaries"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TABLE_XPATH = "/html/body/main/div[2]/section/div/div/div[1]/section/article/div[2]/div/div/div[2]/div/div/div[2]/table"

TEAM_COL = 2  # td[2]
AVG_AGE_COL = 7  # td[7]
TOTAL_CAP_COL = 9  # td[9]


def team_abbrev_from_cell(text: str) -> str | None:
    # usually "SJS", "TBL", etc.
    if not text:
        return None
    m = re.search(r"\b[A-Z]{2,3}\b", text.upper())
    return m.group(0) if m else None


def main():
    driver = webdriver.Safari()

    try:
        for year in YEARS:
            driver.get(BASE_URL.format(year))
            print(year, driver.title)

            # Wait until the table exists
            table = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, TABLE_XPATH))
            )

            # Wait until at least one Total Cap cell exists in the tbody (td[9])
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located(
                    (By.XPATH, f"{TABLE_XPATH}/tbody/tr[1]/td[{TOTAL_CAP_COL}]")
                )
            )

            # Grab all data rows
            rows = table.find_elements(By.XPATH, ".//tbody/tr")
            team_vals, age_vals, cap_vals = [], [], []

            for r in rows:
                tds = r.find_elements(By.XPATH, "./td")
                # need at least 9 columns
                if len(tds) < TOTAL_CAP_COL:
                    continue

                team_cell = tds[TEAM_COL - 1].text.strip()
                age_cell = tds[AVG_AGE_COL - 1].text.strip()
                cap_cell = tds[TOTAL_CAP_COL - 1].text.strip()

                abbr = team_abbrev_from_cell(team_cell)
                if not abbr:
                    continue

                # Skip blank cap rows
                if not cap_cell:
                    continue

                team_vals.append(abbr)
                age_vals.append(age_cell)
                cap_vals.append(cap_cell)

            df = pd.DataFrame({"Team": team_vals, "Avg_Age": age_vals, "Total_Cap": cap_vals})
            out = os.path.join(OUTPUT_DIR, f"team_salary_{year}.csv")
            df.to_csv(out, index=False)
            print(f"{year}: scraped {len(df)} teams -> {out}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
