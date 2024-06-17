import os
from time import perf_counter
import numpy as np
import pandas as pd


# Reads in Kaggle .csv file of NHL stats and performs initial cleaning
def load_data():
    names = ["game_skater_stats", "game_plays", "game_shifts", "game"]
    t2 = perf_counter()
    df = {}

    print("load")
    for name in names:
        df[name] = pd.read_csv(f"kaggle_stats/{name}.csv").drop_duplicates(ignore_index=True)
        t1, t2 = t2, perf_counter()
        print(f"{name:>25}: {t2 - t1:.4g} sec, {len(df[name])} rows")
        # return a dict of df
    return df

if __name__ == "__main__":
    df_master = load_data()
    seasons = [20152016, 20162017, 20172018]
    for season in seasons:
        df = df_master.copy()
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
            ["game_id", "player_id", "team_id"]
        ]
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
            df_corsi.iloc[i, 3:] = [CF, CA, C]
        df_corsi["CF_Percent"] = df_corsi["CF"]/(df_corsi["CF"] + df_corsi["CA"])
        df_corsi.to_csv(f"corsi_vals_II/corsix_{season}.csv")
