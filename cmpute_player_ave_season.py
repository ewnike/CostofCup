import os
import numpy as np
import pandas as pd



#copy helper sql script here read in data from postgres for Corsi_20152016....
#



#create new table with data for player and corsi average
def cmpute_player_ave_season():
    season_averages_list=[]
    seasons = [20152016, 20162017, 20172018]
    
