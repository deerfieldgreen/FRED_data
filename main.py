

import numpy as np
import pandas as pd
import os, sys, re, ast, csv, math, gc, random, enum, argparse, json, requests, time  
from datetime import datetime, timedelta
import matplotlib.pyplot as plt 
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None) # to ensure console display all columns
pd.set_option('display.float_format', '{:0.3f}'.format)
pd.set_option('display.max_row', 50)
plt.style.use('ggplot')
from pathlib import Path
import joblib
from copy import deepcopy

root_folder = "."
projectPath = Path(rf'{root_folder}')

dataPath = projectPath / 'data'
pickleDataPath = dataPath / 'pickle'
configPath = projectPath / 'config'

dataPath.mkdir(parents=True, exist_ok=True)
pickleDataPath.mkdir(parents=True, exist_ok=True)
configPath.mkdir(parents=True, exist_ok=True)

import pickle
def save_obj(obj, name):
    with open(pickleDataPath / f'{name}.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name):
    with open(pickleDataPath / f'{name}.pkl', 'rb') as f:
        return pickle.load(f)






##############################################################################
## Imports

from src.utils import (
    load_config,
)

from fredapi import Fred





##############################################################################
## Settings

config = load_config(path=configPath / "settings.yml")
fred_api_key = config["fred_api_key"]
fred = Fred(api_key=fred_api_key)
data_map_dict = config["data_map_dict"]
col_date = config["col_date"]



##############################################################################
## Main


for data_type in data_map_dict:
    data_ref = data_map_dict[data_type]["data_ref"]
    data_df_init = pd.read_csv(dataPath / data_type / "data.csv")
    data_df_init[col_date] = pd.to_datetime(data_df_init[col_date])

    data_df_new = fred.get_series(data_ref)
    data_df_new = data_df_new.reset_index()
    data_df_new.columns = data_df_init.columns

    ##-##
    data_df_new = data_df_new.head(25287)
    ##-##

    data_df = pd.concat([data_df_init, data_df_new])
    data_df.reset_index(drop=True, inplace=True)

    data_df.drop_duplicates(col_date, keep='first', inplace=True)
    data_df.reset_index(drop=True, inplace=True)

    data_df.sort_values(col_date, ascending=True, inplace=True)
    data_df.reset_index(drop=True, inplace=True)

    data_df.to_csv(dataPath / data_type / "data.csv", index=False)






# 25282 2023-09-19 5.330
# 25283 2023-09-20 5.330
# 25284 2023-09-21 5.330
# 25285 2023-09-22 5.330
# 25286 2023-09-23 5.330

# 25287 2023-09-24 5.330
# 25288 2023-09-25 5.330
# 25289 2023-09-26 5.330
# 25290 2023-09-27 5.330
# 25291 2023-09-28 5.330

















