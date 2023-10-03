

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

##-##
os.chdir(sys.path[0])
##-##



root_folder = "."
projectPath = Path(rf'{root_folder}')

dataPath = projectPath / 'data'
pickleDataPath = dataPath / 'pickle'
configPath = projectPath / 'config'
credsPath = projectPath / 'credentials'

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
## Info

# Need to enable GCP Google Sheets API and Google Drive API, see:
# https://www.analyticsvidhya.com/blog/2020/07/read-and-update-google-spreadsheets-with-python/




##############################################################################
## Imports

from src.utils import (
    load_config,
)

from fredapi import Fred
import gspread
from gspread_dataframe import set_with_dataframe





##############################################################################
## Settings

config = load_config(path=configPath / "settings.yml")
fred_api_key = config["fred_api_key"]
fred = Fred(api_key=fred_api_key)
gspread_client = gspread.service_account(credsPath / 'gsheet' / "creds.json")

data_map_dict = config["data_map_dict"]
col_date = config["col_date"]



##############################################################################
## Main


for data_type in data_map_dict:
    data_ref = data_map_dict[data_type]["data_ref"]
    spreadsheet_id = data_map_dict[data_type]["spreadsheet_id"]

    data_df_init = pd.read_csv(dataPath / data_type / "data.csv")
    data_df_init[col_date] = pd.to_datetime(data_df_init[col_date])

    if data_ref not in data_df_init.columns:
        print(f"# {data_type}: data column not found !!")
        continue

    data_df_new = fred.get_series(data_ref)
    data_df_new = data_df_new.dropna()
    data_df_new = data_df_new.reset_index()
    # data_df_new.columns = data_df_init.columns
    data_df_new.columns = [col_date, data_ref]

    data_df = pd.concat([data_df_init, data_df_new])
    data_df.reset_index(drop=True, inplace=True)

    data_df.drop_duplicates(col_date, keep='first', inplace=True)
    data_df.reset_index(drop=True, inplace=True)

    data_df.sort_values(col_date, ascending=True, inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    data_df[col_date] = data_df[col_date].dt.date
    data_df.to_csv(dataPath / data_type / "data.csv", index=False)

    workbook = gspread_client.open_by_key(spreadsheet_id)
    worksheet = workbook.worksheet(data_ref)
    worksheet.clear()
    set_with_dataframe(
        worksheet=worksheet, dataframe=data_df, include_index=False,
        include_column_header=True, resize=True,
    )

    print(f"# {data_type}: Updated")




