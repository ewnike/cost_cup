import os
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Set up the Safari WebDriver
driver = webdriver.Safari()

# Base URL to scrape
base_url = "https://www.spotrac.com/nhl/rankings/player/_/year/{}/sort/cap_total"

# Years to scrape
years = [2015, 2016, 2017]

# Directory to store CSV files
output_dir = "player_cap_hits"
os.makedirs(output_dir, exist_ok=True)

# Dictionary to store DataFrames for each year
dfs_by_year = {}


# Function to clean and split player name
def split_player_name(name):
    name_parts = name.split()
    first_name = name_parts[0]
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    return first_name, last_name


# Loop through each year and scrape the data
for year in years:
    url = base_url.format(year)
    driver.get(url)

    # Wait until the table is loaded
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "list-group-item"))
    )

    # Scroll to the bottom of the page to load all content (if applicable)
    while True:
        previous_height = driver.execute_script("return document.body.scrollHeight")
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(2)  # Wait for new data to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == previous_height:
            break  # Exit the loop when no more new content is loaded

    # Get page source and parse with BeautifulSoup
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Find the elements containing the player names and cap hits
    first_names = []
    last_names = []
    cap_hits = []

    for item in soup.find_all("li", class_="list-group-item"):
        name_div = item.find("div", class_="link")
        cap_hit_span = item.find("span", class_="medium")
        if name_div and cap_hit_span:
            name = name_div.text.strip()
            first_name, last_name = split_player_name(name)
            cap_hit = cap_hit_span.text.strip()
            first_names.append(first_name)
            last_names.append(last_name)
            cap_hits.append(cap_hit)

    # Create a DataFrame to store the results
    df = pd.DataFrame(
        {"First Name": first_names, "Last Name": last_names, "Cap Hit": cap_hits}
    )

    # Store the DataFrame in the dictionary
    dfs_by_year[year] = df

# Close the driver
driver.quit()

# Display the DataFrames for each year
for year, df in dfs_by_year.items():
    print(f"Data for {year}:")
    print(df.head(), "\n")

# Save the DataFrames to CSV files
for year, df in dfs_by_year.items():
    csv_path = os.path.join(output_dir, f"player_cap_hits_{year}.csv")
    df.to_csv(csv_path, index=False, mode="w")

print(f"Data saved to {output_dir} directory.")