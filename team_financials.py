import numpy as np
import pandas as pd
import requests


def clean_spo_df(df):
    df.columns = [
        "Rank",
        "Team",
        "Record",
        "Players Active",
        "Avg Age Team",
        "Total Cap Allocations",
        "Long-Term IR Adjustment",
        "Cap Space All",
        "Active",
        "Injured",
        "Injured  Long-Term",
    ]
    df = df[["Rank", "Team", "Total Cap Allocations", "Cap Space All"]]
    df_trimmed = df.iloc[:-2].copy()
    # Splitting the "Team" column based on the first occurence of a space and removing duplicate from "Team"
    df_trimmed[["Team", "Drop"]] = df_trimmed["Team"].str.split(" ", n=1, expand=True)
    # Dropping the column "Drop"
    df_trimmed = df_trimmed.drop(columns=["Drop"])

    return df_trimmed


def clean_capfr_df(df):
    df = df[["PLAYER", "TEAM", "POS", "CAP HIT", "SALARY"]]
    # NEW LINES: Splitting "PLAYER" Column into first and last names
    # Splitting the "PLAYER" column based on the first occurrence of a space
    df[["prefix", "firstName", "lastName"]] = df["PLAYER"].str.split(
        " ", n=2, expand=True
    )
    # Drop the original "PLAYER" column and the "prefix" column
    df = df.drop(columns=["PLAYER", "prefix"])

    return df


# Creates a pandas dataframe from a website table given a dictionary of URLs
def read_url(urls):
    total_dfs = []

    for url in urls.keys():
        if urls[url] == "single":
            df = pd.read_html(url)[0]
            df = clean_spo_df(df)
            total_dfs.append(df)
        if urls[url] == "multi":
            dfs = []
            i = "1"
            df = pd.read_html(url + i)[0]
            while len(df) != 0:
                df = clean_capfr_df(df)
                dfs.append(df)
                df = pd.read_html(url + i)[0]
                i = int(i)
                i += 1
                i = str(i)
            combined_df = pd.DataFrame()
            for df in dfs:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            total_dfs.append(combined_df)

    return total_dfs


# Writes .csv files from dfs given a list structured as [type, year, df]
def write_csv(dfs):
    for df in dfs:
        df[2].to_csv(df[0] + "_files/" + df[0] + "_" + df[1] + ".csv")
    return


def main():
    print("Running Main...")

    # Spotrac URLs for team salary totals
    spo_url_15 = "https://www.spotrac.com/nhl/cap/_/year/2015/sort/cap_maximum_space2"
    spo_url_16 = "https://www.spotrac.com/nhl/cap/_/year/2016/sort/cap_maximum_space2"
    spo_url_17 = "https://www.spotrac.com/nhl/cap/_/year/2017/sort/cap_maximum_space2"

    # Cap Friendly URLs for player salary totals
    cafr_base_15 = "https://www.capfriendly.com/browse/active/2016?hide=clauses,age,handed,skater-stats,goalie-stats&pg="
    cafr_base_16 = "https://www.capfriendly.com/browse/active/2017?hide=clauses,age,handed,skater-stats,goalie-stats&pg="
    cafr_base_17 = "https://www.capfriendly.com/browse/active/2018?hide=clauses,age,handed,skater-stats,goalie-stats&pg="

    nhl_urls = {
        spo_url_15: "single",
        spo_url_16: "single",
        spo_url_17: "single",
        cafr_base_15: "multi",
        cafr_base_16: "multi",
        cafr_base_17: "multi",
    }

    (
        team_sals_15,
        team_sals_16,
        team_sals_17,
        player_sals_15,
        player_sals_16,
        player_sals_17,
    ) = read_url(nhl_urls)

    dfs = [
        ["team", "20152016", team_sals_15],
        ["team", "20162017", team_sals_16],
        ["team", "20172018", team_sals_17],
        ["player", "20152016", player_sals_15],
        ["player", "20162017", player_sals_16],
        ["player", "20172018", player_sals_17],
    ]

    write_csv(dfs)


if __name__ == "__main__":
    main()
