from time import perf_counter
import numpy as np
import pandas as pd


# Reads in Kaggle .csv file of NHL stats and performs initial cleaning
def load_data():
    names = ["game_skater_stats", "game_plays", "game_shifts", "game", "player_info", "team_info"]
    t2 = perf_counter()
    df = {}

    print("load")
    for name in names:
        df[name] = pd.read_csv(f"kaggle_stats/{name}.csv").drop_duplicates(ignore_index=True)
        t1, t2 = t2, perf_counter()
        print(f"{name:>25}: {t2 - t1:.4g} sec, {len(df[name])} rows")
        # return a dict of df
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
            while len(df) != 0:
                # print("printing url")
                # print(url)
                # print(url + i)
                df = clean_capfr_df(df)
                dfs.append(df)
                df = pd.read_html(url + i)[0]
                i = int(i)
                i += 1
                i = str(i)
                print("Moving to next page")
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


# Cleans a Spotrac dataframe
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
    print("printing df before cleaning")
    print(df.head())
    df = df[["Rank", "Team", "Total Cap Allocations", "Cap Space All"]]
    df_trimmed = df.iloc[:-2]
    print("printing trimmed df")
    print(df_trimmed.head())
    return df_trimmed


# cleans a Cap Friendly dataframe
def clean_capfr_df(df):
    # columns = df.columns
    # print("Columns as Index object:", columns)
    df = df[["PLAYER", "TEAM", "POS", "CAP HIT", "SALARY"]]
    print(df.head())
    print("printing Player Name")
    # Splitting the "PLAYER" column based on the first occurrence of a space
    df[["prefix", "firstName", "lastName"]] = df["PLAYER"].str.split(" ", n=2, expand=True)
    print("Testing")
    # Drop the original "PLAYER" column and the "prefix" column
    df = df.drop(columns=["PLAYER", "prefix"])

    return df


# Breaks NHL dataframe down into individual seasons
def organize_by_season(seasons, df):
    df_orig = df
    nhl_dfs = []
    for season in seasons:
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season}")
        # filter games to just one season
        # when we call df, we are actually calling the keys in the dict of df and this is why we can now call df[]as opposed to df_game....
        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            # do an inner merge to reduce the number of rows...keeping only the rows where game and game_id match ....
            df[name] = pd.merge(df[name], df["game"][["game_id"]], on="game_id")
            for key, val in df.items():
                print(f"{key:>25}: {len(val)}")
        # reduce df['game_plays'] df in advance
        cols = ["play_id", "game_id", "team_id_for", "event", "time"]
        events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
        # using .loc here as a mask
        df["game_plays"] = df["game_plays"].loc[df["game_plays"]["event"].isin(events)]
        # defining "time" col
        df["game_plays"]["time"] = (
            df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
        )
        df["game_plays"] = df["game_plays"][cols]

        print(f"reduced game_plays num rows: {len(df['game_plays'])}")

        # initialize corsi df
        # sort all rows by game_id and on ties defer to player_id... everything with the same game_id will be grouped together
        df_corsi = df["game_skater_stats"].sort_values(["game_id", "player_id"], ignore_index=True)[
            ["game_id", "player_id", "team_id", "timeOnIce"]
        ]

        nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])

    return nhl_dfs


# Takes a list of pandas dataframes, calculates corsi statistics and adds them to dataframes
def create_corsi_stats(df_corsi, df):
    df_corsi[["CF", "CA", "C"]] = np.nan

    game_id_prev = None
    t1 = perf_counter()
    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row.iloc[:3]
        if i % 1000 == 0:
            print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")
        if game_id != game_id_prev:
            shifts_game = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = df["game_plays"].query(f"game_id == {game_id}")
        shifts_player = shifts_game.query(f"player_id == {player_id}")
        mask = (
            shifts_game["shift_start"].searchsorted(plays_game["time"])
            - shifts_game["shift_end"].searchsorted(plays_game["time"])
        ).astype(bool)
        plays_player = plays_game[mask]
        # mask was it for or against our team. is it for team of the player whose player_id we are looking at
        is_our_team = plays_player["team_id_for"] == team_id
        is_missed_shot = plays_player["event"] == "Missed Shot"
        CF = (is_our_team ^ is_missed_shot).sum()
        # number of rows in the df
        CA = len(plays_player) - CF
        C = CF - CA
        df_corsi.iloc[i, 4:] = [CF, CA, C]
    df_corsi["CF_Percent"] = df_corsi["CF"] / (df_corsi["CF"] + df_corsi["CA"])

    # Merging player_info and team_info
    df_corsi = df_corsi.merge(
        df["player_info"][["player_id", "firstName", "lastName", "primaryPosition"]],
        on="player_id",
        how="left",
    )
    df_corsi = df_corsi.merge(
        df["team_info"][["team_id", "teamName", "abbreviation"]], on="team_id", how="left"
    )

    return df_corsi


def join_dfs(season_dfs, salary_dfs):
    joined_dfs = []

    if len(season_dfs) == len(salary_dfs):
        for season_df, salary_df in zip(season_dfs, salary_dfs):
            # Merge season_df with salary_df based on firstName and lastName
            merged_df = pd.merge(
                season_df[1],
                salary_df[["firstName", "lastName", "CAP HIT", "SALARY"]],
                on=["firstName", "lastName"],
                how="left",
            )

            # Append the merged dataframe to joined_dfs
            joined_dfs.append([season_df[0], merged_df])

    return joined_dfs


# Writes csv files for individual NHL seasons from a list of pandas dataframes
def write_csv(dfs):
    for df in dfs:
        df[1].to_csv(f"TEMP_corsi_vals/TEMP_Corsi_{df[0]}.csv", index=False)


def main():
    # Spotrac URLs for team salary totals
    spo_url_15 = "https://www.spotrac.com/nhl/cap/_/year/2015/sort/cap_maximum_space2"
    spo_url_16 = "https://www.spotrac.com/nhl/cap/_/year/2016/sort/cap_maximum_space2"
    spo_url_17 = "https://www.spotrac.com/nhl/cap/_/year/2017/sort/cap_maximum_space2"

    # Cap Friendly URLs for player salary totals
    cafr_base_15 = "https://www.capfriendly.com/browse/active/2016?hide=clauses,age,handed,skater-stats,goalie-stats&pg="
    cafr_base_16 = "https://www.capfriendly.com/browse/active/2017?hide=clauses,age,handed,skater-stats,goalie-stats&pg="
    cafr_base_17 = "https://www.capfriendly.com/browse/active/2018?hide=clauses,age,handed,skater-stats,goalie-stats&pg="

    # Loading Kaggle files
    df_master = load_data()
    seasons = [20152016, 20162017, 20172018]
    nhl_dfs = organize_by_season(seasons, df_master)

    nhl_urls = {
        spo_url_15: "single",
        spo_url_16: "single",
        spo_url_17: "single",
        cafr_base_15: "multi",
        cafr_base_16: "multi",
        cafr_base_17: "multi",
    }

    team_sals_15, team_sals_16, team_sals_17, player_sals_15, player_sals_16, player_sals_17 = (
        read_url(nhl_urls)
    )

    salary_dfs = [player_sals_15, player_sals_16, player_sals_17]
    final_dfs = join_dfs(nhl_dfs, salary_dfs)

    write_csv(final_dfs)


if __name__ == "__main__":
    main()
