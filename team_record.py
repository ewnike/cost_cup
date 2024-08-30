"""
August 28, 2024
Script to scrape team win/loss record from hockey-reference.com
Used selenium.xpath and set webdriver to safari.
Eric Winiecke
"""

import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.safari.service import Service as SafariService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Directory to store CSV files
OUTPUT_DIR = "team_records"
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
    "team_name": "Team",
    "games": "GP",
    "wins": "W",
    "losses": "L",
    "losses_ot": "OTL",
    "points": "PTS",
}

# XPath to locate specific columns in the table
xpaths = {
    "team_name": './/td[@data-stat="team_name"]/a',
    "games": './/td[@data-stat="games"]',
    "wins": './/td[@data-stat="wins"]',
    "losses": './/td[@data-stat="losses"]',
    "losses_ot": './/td[@data-stat="losses_ot"]',
    "points": './/td[@data-stat="points"]',
}

# Step 2: Create a dictionary mapping full names to abbreviations and team IDs
name_to_abbreviation = {
    "Washington Capitals": ("WAS", 15),
    "Dallas Stars": ("DAL", 25),
    "St. Louis Blues": ("STL", 19),
    "Pittsburgh Penguins": ("PIT", 5),
    "Chicago Blackhawks": ("CHI", 16),
    "Florida Panthers": ("FLA", 13),
    "Anaheim Ducks": ("ANA", 24),
    "Los Angeles Kings": ("LAK", 26),
    "New York Rangers": ("NYR", 3),
    "New York Islanders": ("NYI", 2),
    "San Jose Sharks": ("SJS", 28),
    "Tampa Bay Lightning": ("TBL", 14),
    "Nashville Predators": ("NSH", 18),
    "Philadelphia Flyers": ("PHI", 4),
    "Detroit Red Wings": ("DET", 17),
    "Boston Bruins": ("BOS", 6),
    "Minnesota Wild": ("MIN", 30),
    "Carolina Hurricanes": ("CAR", 12),
    "Ottawa Senators": ("OTT", 9),
    "New Jersey Devils": ("NJD", 1),
    "Montreal Canadiens": ("MTL", 8),
    "Colorado Avalanche": ("COL", 21),
    "Buffalo Sabres": ("BUF", 7),
    "Winnipeg Jets": ("WPG", 52),
    "Arizona Coyotes": ("ARI", 53),
    "Calgary Flames": ("CGY", 20),
    "Columbus Blue Jackets": ("CBJ", 29),
    "Vancouver Canucks": ("VAN", 23),
    "Edmonton Oilers": ("EDM", 22),
    "Toronto Maple Leafs": ("TOR", 10),
}

# Convert the dictionary into a DataFrame
team_df = pd.DataFrame.from_dict(
    name_to_abbreviation, orient="index", columns=["Abbreviation", "Team_ID"]
)
team_df.reset_index(inplace=True)
team_df.rename(columns={"index": "Team_Name"}, inplace=True)

# Display the team DataFrame
print("Team DataFrame:")
print(team_df)

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

    # Merge the scraped data with the team_df to include Abbreviation and Team_ID
    merged_df = pd.merge(df, team_df, left_on="Team", right_on="Team_Name", how="left")

    # Drop the redundant 'Team_Name' column from the merged DataFrame
    merged_df.drop(columns=["Team_Name"], inplace=True)

    # Save the merged DataFrame to a CSV file for the current year
    output_file = os.path.join(OUTPUT_DIR, f"NHL_{year}_team_stats.csv")
    merged_df.to_csv(output_file, index=False)

    print(f"Scraping and merging completed for {year}. Data saved to '{output_file}'")

# Close the Safari WebDriver
driver.quit()

print("All seasons scraped, merged, and saved.")
