

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
    data_df[col_date] = data_df[col_date].dt.date
    data_df.to_csv(dataPath / data_type / "data.csv", index=False)

    workbook = gspread_client.open_by_key(spreadsheet_id)
    worksheet = workbook.worksheet(data_ref)
    worksheet.clear()
    set_with_dataframe(
        worksheet=worksheet, dataframe=data_df, include_index=False,
        include_column_header=True, resize=True,
    )

    print(f"# Updated: {data_type}")

# 
# spreadsheet_id = "1K_NEb7HIIY0X5aCJ1kzrJh5Ieor-ACloU_V6VDotpzI"
# 
# # spreadsheet_id = "15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM"
# workbook = gspread_client.open_by_key(spreadsheet_id)
# worksheet = workbook.worksheet(data_ref)
# 
# 
# data_df = data_df.head(25250)
# 
# 
# 
# worksheet.clear()
# set_with_dataframe(
#     worksheet=worksheet, dataframe=data_df, include_index=False,
#     include_column_header=True, resize=True,
# )
# 
# 
# 
# 2023-08-13	5.33
# 2023-08-14	5.33
# 2023-08-15	5.33
# 2023-08-16	5.33
# 2023-08-17	5.33
# 
# 2023-09-19	5.33
# 2023-09-20	5.33
# 2023-09-21	5.33
# 2023-09-22	5.33
# 2023-09-23	5.33
# 
# # gsheets@decoded-pilot-399603.iam.gserviceaccount.com
# 
# 
# 
# #######################################################################################################
# 
# 
# 
# 
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# 
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# SPREADSHEET_ID = '1kvHv1OBCzr9GnFxRu9RTJC7jjQjc9M4rAiDnhyak2Sg'
# 
# CREDENTIALS_PATH = ('./gsheet/creds.json')
# 
# def _get_sheets_service_client():
#   creds = service_account.Credentials.from_service_account_file(
#       CREDENTIALS_PATH, scopes=SCOPES)
#   service = build('sheets', 'v4', credentials=creds)
#   return service
# 
# 
# 
# # 25282 2023-09-19 5.330
# # 25283 2023-09-20 5.330
# # 25284 2023-09-21 5.330
# # 25285 2023-09-22 5.330
# # 25286 2023-09-23 5.330
# 
# # 25287 2023-09-24 5.330
# # 25288 2023-09-25 5.330
# # 25289 2023-09-26 5.330
# # 25290 2023-09-27 5.330
# # 25291 2023-09-28 5.330
# 
# 
# 
# 
# 
# 
# 
# 
# CREDENTIALS_PATH = credsPath / 'gsheet' / "creds.json"
# 
# 
# 
# 
# 
# 
# 
# 
# 
# from google.oauth2 import service_account
# import gspread
# 
# scopes = [
#     'https://www.googleapis.com/auth/spreadsheets',
# ]
# creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
# 
# 
# 
# 
# 
# gc.open_by_key(SPREADSHEET_ID)
# 
# 
# 
# 
# 
# 
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# 
# SCOPES = [
#     'https://www.googleapis.com/auth/drive',
#     'https://www.googleapis.com/auth/spreadsheets',
# ]
# SPREADSHEET_ID = "15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM"
# CREDENTIALS_PATH = credsPath / 'gsheet' / "creds.json"
# 
# credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
# service = build('sheets', 'v4', credentials=credentials)
# client = gspread.authorize(credentials)
# 
# 
# spreadsheet_response = service.spreadsheets().values().get(
#   spreadsheetId=SPREADSHEET_ID,
#   range='{}!A1:A'.format("Sheet1")).execute()
# entries = len(spreadsheet_response['values'])
# 
# 
# client = gspread.authorize(credentials)
# 
# 
# 
# 
# 
# 
# 
# url = "https://docs.google.com/spreadsheets/d/15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM"
# 
# url = "https://docs.google.com/spreadsheets/d/15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM/edit?usp=sharing"
# 
# 
# 
# url = SPREADSHEET_ID
# client.open(SPREADSHEET_ID)
# 
# client.open(url)
# 
# 
# 
# 
# service = build('sheets', 'v4', credentials=credentials)
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# import google.auth
# import gspread
# 
# scopes = [
#     # 'https://spreadsheets.google.com/feeds',
#     # 'https://www.googleapis.com/auth/spreadsheets.readonly',
#     'https://www.googleapis.com/auth/drive',
#     'https://www.googleapis.com/auth/drive.file',
#     'https://www.googleapis.com/auth/spreadsheets',
# ]
# 
# # scopes = ['https://spreadsheets.google.com/feeds']
# 
# credentials, project_id = google.auth.default(
#     scopes=scopes
# )
# 
# client = gspread.authorize(credentials)
# 
# 
# spreadsheetId = "15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM"
# 
# url = "https://docs.google.com/spreadsheets/d/15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM/edit?usp=sharing"
# 
# 
# url = "https://docs.google.com/spreadsheets/d/15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM"
# 
# url = spreadsheetId
# client.open(url)
# 
# 
# sheet = client.open(spreadsheet).worksheet(workbook)
# 
# 694048520418-compute@developer.gserviceaccount.com
# 
# 694048520418-compute@developer.gserviceaccount.com
# 
# 
# 
# os.listdir(credsPath)
# 
# 
# 
# 
# import google.auth
# from googleapiclient.discovery import build
# 
# 
# 
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# 
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# SPREADSHEET_ID = "15-Xw5H-JkiYeYhDtlnWl8EvSRry7yczWSsqvBCwuFlM"
# CREDENTIALS_PATH = credsPath / 'gsheet' / "creds.json"
# 
# creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
# service = build('sheets', 'v4', credentials=creds)
# 
# 
# 
# 
# 
# 
# spreadsheet_response = service.spreadsheets().values().get(
#   spreadsheetId=SPREADSHEET_ID,
#   range='{}!A1:A'.format("Sheet1")).execute()
# entries = len(spreadsheet_response['values'])
# 
# 
# 
# 
# 
# 
# 
# def _get_sheets_service_client():
#   creds = service_account.Credentials.from_service_account_file(
#       CREDENTIALS_PATH, scopes=SCOPES)
#   service = build('sheets', 'v4', credentials=creds)
#   return service
# 
# 
# 
