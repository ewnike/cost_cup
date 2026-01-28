"""
August 11, 2024.

Script to scrape payer caphit, first name, and
last name from spotrac. Used selenium and set webdriver
to safari.

Eric Winiecke.
"""

import os
import re

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Import the shared scrolling function
from .scraping_utils import scroll_to_bottom

# Set up the Safari WebDriver
driver = webdriver.Safari()

# Base URL to scrape
BASE_URL = "https://www.spotrac.com/nhl/rankings/player/_/year/{}/sort/cap_total"

# Years to scrape
# years = [2015, 2016, 2017]
years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

# Directory to store CSV files
OUTPUT_DIR = "player_cap_hits"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dictionary to store DataFrames for each year
dfs_by_year = {}


def split_player_name(name):
    """Clean data and split player name. Returns (None, None) if unusable."""
    if name is None:
        return None, None

    cleaned = re.sub(r"\s+", " ", str(name)).strip()
    if not cleaned:
        return None, None

    parts = cleaned.split(" ")
    first_name = parts[0]
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first_name, last_name


# Loop through each year and scrape the data
for year in years:
    url = BASE_URL.format(year)
    driver.get(url)

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "list-group-item"))
    )

    scroll_to_bottom(driver, wait_time=2)

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    first_names = []
    last_names = []
    cap_hits = []
    spotrac_urls = []  # ✅ reset per year

    for item in soup.find_all("li", class_="list-group-item"):
        name_div = item.find("div", class_="link")
        cap_hit_span = item.find("span", class_="medium")

        if not name_div or not cap_hit_span:
            continue

        a_tag = name_div.find("a")
        spotrac_url = a_tag["href"] if a_tag and a_tag.has_attr("href") else None
        if spotrac_url and spotrac_url.startswith("/"):
            spotrac_url = "https://www.spotrac.com" + spotrac_url

        name = name_div.get_text(" ", strip=True)
        if not name:
            continue

        first_name, last_name = split_player_name(name)
        if not first_name:
            continue

        cap_hit = cap_hit_span.get_text(" ", strip=True)
        cap_hit_num = cap_hit.replace("$", "").replace(",", "").replace("—", "").strip()
        cap_hit_num = float(cap_hit_num) if cap_hit_num else None
        if cap_hit_num is None:
            continue

        first_names.append(first_name)
        last_names.append(last_name)
        cap_hits.append(cap_hit_num)
        spotrac_urls.append(spotrac_url)

    # ✅ build df once, after scraping the year
    df = pd.DataFrame(
        {
            "firstName": first_names,
            "lastName": last_names,
            "capHit": cap_hits,
            "spotrac_url": spotrac_urls,
        }
    )

    dfs_by_year[year] = df

    print(f"{year}: scraped {len(df)} players")
    dupes = df.duplicated(subset=["firstName", "lastName"]).sum()
    print(f"{year}: dupes by name = {dupes}")

    dupe_rows = df[df.duplicated(subset=["firstName", "lastName"], keep=False)].sort_values(
        ["lastName", "firstName"]
    )
    if not dupe_rows.empty:
        print(f"{year} duplicate names:")
        print(dupe_rows)

driver.quit()

for year, df in dfs_by_year.items():
    csv_path = os.path.join(OUTPUT_DIR, f"player_cap_hits_{year}.csv")
    df.to_csv(csv_path, index=False, mode="w")

print(f"Data saved to {OUTPUT_DIR} directory.")
