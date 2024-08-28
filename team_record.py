import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.safari.service import Service as SafariService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Directory to store CSV files
OUTPUT_DIR = "output_dir"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set up the Selenium WebDriver for Safari
service = SafariService()
driver = webdriver.Safari(service=service)

# Base URL to scrape
BASE_URL = "https://www.hockey-reference.com/leagues/NHL_{}.html"

# Years to scrape
years = [2016, 2017, 2018]

# Columns to extract
columns_to_extract = {
    'team_name': 'Team',
    'games': 'GP',
    'wins': 'W',
    'losses': 'L',
    'losses_ot': 'OTL',
    'points': 'PTS'
}

# XPath to locate specific columns in the table
xpaths = {
    'team_name': './/td[@data-stat="team_name"]/a',
    'games': './/td[@data-stat="games"]',
    'wins': './/td[@data-stat="wins"]',
    'losses': './/td[@data-stat="losses"]',
    'losses_ot': './/td[@data-stat="losses_ot"]',
    'points': './/td[@data-stat="points"]'
}

# Loop through each year and scrape the data
for year in years:
    # Generate the URL for the current year
    url = BASE_URL.format(year)

    # Navigate to the URL
    driver.get(url)

    # Wait until the table is loaded
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="stats"]/tbody/tr'))
    )

    # Locate the rows in the table
    rows = driver.find_elements(By.XPATH, '//*[@id="stats"]/tbody/tr')

    # Prepare the list to hold the data
    data = []

    # Iterate over each row and extract the specific columns
    for row in rows:
        team_data = {}
        for key, xpath in xpaths.items():
            element = row.find_element(By.XPATH, xpath)
            team_data[columns_to_extract[key]] = element.text
        data.append(team_data)

    # Convert the data to a DataFrame
    df = pd.DataFrame(data, columns=columns_to_extract.values())

    # Save the DataFrame to a CSV file for the current year
    output_file = os.path.join(OUTPUT_DIR, f"NHL_{year}_team_stats.csv")
    df.to_csv(output_file, index=False)

    print(f"Scraping completed for {year}. Data saved to '{output_file}'")

# Close the Safari WebDriver
driver.quit()

print("All seasons scraped and saved.")







