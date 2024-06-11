import pandas as pd
import numpy as np


def read_data():
    game_plays = pd.read_csv("/kaggle/input/nhl-game-data/game_plays.csv").drop_duplicates(
        ignore_index=True
    )
    game_shifts = pd.read_csv("/kaggle/input/nhl-game-data/game_shifts.csv").drop_duplicates(
        ignore_index=True
    )
    # game_plays_players = pd.read_csv("/kaggle/input/nhl-game-data/game_plays_players.csv")
    game_skater_stats = pd.read_csv(
        "/kaggle/input/nhl-game-data/game_skater_stats.csv"
    ).drop_duplicates(ignore_index=True)
    # we are retruning a tuple!
    return game_plays, game_shifts, game_skater_stats


def filter_shifts(game_shifts, game_id, player_id):
    return game_shifts.query(f"game_id=={game_id} and player_id=={player_id}")
    # f-string prints variable out into the string


if __name__ == "__main__":
    # running the function
    game_plays, game_shifts, game_skater_stats = read_data()
    dfc_out = game_skater_stats.copy()
    # appending extra columns to the right of dfc_out and using nan as placeholders
    dfc_out[["CF", "CA", "C"]] = np.nan
    events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]
    mask = game_plays["event"].isin(events)
    game_plays = game_plays.loc[mask]
    # i gives you index of the row and row is a pandas series object of that row
    for i, row in dfc_out.iterrows():
        game_id = row["game_id"]
        player_id = row["player_id"]
        team_id = row["team_id"]
        # The next section we are filtering the game data to just player and game and adding the player id.
        # looking for events from the 1 game
        df_i = game_plays.query(f"game_id=={game_id}")
        # team_id = game_skater_stats.query(f"game_id=={game_id} and player_id=={player_id}").iloc[0,2]#indexing funct that uses brackets and numerals(i.e. an index)
        shifts = filter_shifts(game_shifts, game_id, player_id)
        # making time consistent with othe table columns
        df_i["time"] = df_i["periodTime"] + (df_i["period"] - 1) * 1200
        # detect when player is on the ice for an event
        mask = (
            shifts["shift_start"].searchsorted(df_i["time"])
            - shifts["shift_end"].searchsorted(df_i["time"])
        ).astype(bool)
        df_i = df_i.loc[mask]
        mask_tid4 = df_i["team_id_for"] == team_id
        mask_msshot = df_i["event"] == "Missed Shot"
        CF = (mask_tid4 & ~mask_msshot).sum() + (~mask_tid4 & mask_msshot).sum()
        CA = df_i.shape[0] - CF
        C = CF - CA
        # i gives us the index for the for the row that we are putting our stats into
        dfc_out.loc[i, ["CF", "CA", "C"]] = [CF, CA, C]
    dfc_out
