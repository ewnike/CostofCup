import requests
import pandas as pd
import numpy as np


def clean_spo_df(df):
    # columns = df.columns
    # print("Columns as Index object:", columns)
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
    df_trimmed = df.iloc[:-2]
    return df_trimmed


# Cleans a pandas dataframe from a Cap Friendly URL
def clean_capfr_df(df):
    # columns = df.columns
    # print("Columns as Index object:", columns)
    df = df[["PLAYER", "TEAM", "POS", "CAP HIT", "SALARY"]]
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
            print("Creating list of dfs to concatenate")
            df = pd.read_html(url + i)[0]
            while len(df) > 0:
                # print("printing url")
                # print(url)
                # print(url + i)
                df = pd.read_html(url + i)[0]
                i = int(i)
                # print(i)
                i += 1
                # print(i)
                i = str(i)
                # print(i)
                df = clean_capfr_df(df)
                dfs.append(df)
            print("Finished creating list of dfs to concatenate")
            combined_df = pd.DataFrame()
            for df in dfs:
                # print(type(combined_df))
                # print(type(df))
                # print("Printing next page of df to concatenate")
                # print(df.head())
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                # print("Printing updated concatenated df with next page")
                # print(combined_df.head())
            # print("Printing fully concatenated df and its length")
            # print(combined_df.head())
            # print(len(combined_df))
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

team_sals_15, team_sals_16, team_sals_17, player_sals_15, player_sals_16, player_sals_17 = read_url(
    nhl_urls
)

dfs = [
    ["team", "20151016", team_sals_15],
    ["team", "20162017", team_sals_16],
    ["team", "20171018", team_sals_17],
    ["player", "20151016", player_sals_15],
    ["player", "20161017", player_sals_16],
    ["player", "20171018", player_sals_17],
]

write_csv(dfs)

print(team_sals_15.head())
print(team_sals_16.head())
print(team_sals_17.head())
print(player_sals_15.head())
print(player_sals_16.head())
print(player_sals_17.head())


if __name__ == "__main__":
    main()