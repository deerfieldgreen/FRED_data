from io import StringIO

import numpy as np
import pandas as pd
import os, sys, re, ast, csv, math, gc, random, enum, argparse, json, requests, time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)  # to ensure console display all columns
pd.set_option('display.float_format', '{:0.3f}'.format)
pd.set_option('display.max_row', 50)
plt.style.use('ggplot')
from pathlib import Path
from github import Github, GithubException
import joblib
from copy import deepcopy
from src.utils import read_and_encode_file, get_gcp_bucket
from huggingface_hub import HfApi, HfFolder
from datasets import Dataset

##-##
# os.chdir(sys.path[0])
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

##############################################################################
## Imports

from src.utils import (
    load_config,
    del_file,
)

from fredapi import Fred
import dotenv

dotenv.load_dotenv(".env")

PUSH_TO_GITHUB = False if os.environ.get("PUSH_TO_GITHUB") == 'False' else True
PUSH_TO_HF = False if os.environ.get("PUSH_TO_HF") == 'False' else True
PUSH_TO_GCP = False if os.environ.get("PUSH_TO_GCP") == 'False' else True

##############################################################################
## Settings

config = load_config(path=configPath / "settings.yml")
fred_api_key = os.environ.get("FRED_API_KEY")
fred = Fred(api_key=fred_api_key)

bucket = get_gcp_bucket()

data_map_dict = config["data_map_dict"]
col_date = config["col_date"]

###########################################################################
token = os.environ.get("GIT_TOKEN")
g = Github(token)
repo = g.get_repo(
    "deerfieldgreen/FRED_data"
)  # Replace with your repo details

hf_token = os.environ.get("HF_API_KEY")
hf_api = HfApi(token=hf_token)
print(hf_api.whoami())
hf_user = "deerfieldgreen"  # Replace with your Hugging Face repo details

##############################################################################
## Main

for data_type in data_map_dict:

    # if data_type != 'total_public_debt':
    #     continue

    data_ref = data_map_dict[data_type]["data_ref"]
    spreadsheet_id = data_map_dict[data_type]["spreadsheet_id"]
    data_source = data_map_dict[data_type]["data_source"]

    csv_file = dataPath / data_type / "data.csv"
    if not os.path.exists(csv_file):
        data_df_init = pd.DataFrame(columns=[col_date, data_ref])
    else:
        data_df_init = pd.read_csv(dataPath / data_type / "data.csv")
    data_df_init[col_date] = pd.to_datetime(data_df_init[col_date])

    # if data_ref not in data_df_init.columns:
    #     print(f"# {data_type}: data column not found !!")
    #     continue

    if data_source == "FRED":
        data_df_new = fred.get_series(data_ref)
        data_df_new = data_df_new.dropna()
        data_df_new = data_df_new.reset_index()
        data_df_new.columns = [col_date, data_ref]

    if data_source == "SPGLOBAL":
        url = data_map_dict[data_type]["url"]
        filename = "TEMP_data.xls"
        del_file(dataPath / filename)

        # urlretrieve(url, dataPath / filename)
        # headers = {'user-agent': 'Mozilla/5.0'}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'http://www.spglobal.com/',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        r = requests.get(url, headers=headers)
        with open(dataPath / filename, 'wb') as f:
            f.write(r.content)

        data_df_new = pd.read_excel(dataPath / filename, engine='openpyxl')
        data_df_new.columns = [col_date, data_ref]
        data_df_new = data_df_new.dropna()
        data_df_new.reset_index(drop=True, inplace=True)
        data_df_new = data_df_new.iloc[3:]
        data_df_new.reset_index(drop=True, inplace=True)
        data_df_new[col_date] = pd.to_datetime(data_df_new[col_date])
        data_df_new[data_ref] = data_df_new[data_ref].astype(float)
        del_file(dataPath / filename)

    data_df = pd.concat([data_df_init, data_df_new])
    data_df.reset_index(drop=True, inplace=True)

    data_df.drop_duplicates(col_date, keep='first', inplace=True)
    data_df.reset_index(drop=True, inplace=True)
    data_df.sort_values(col_date, ascending=True, inplace=True)
    data_df.reset_index(drop=True, inplace=True)

    data_df[col_date] = data_df[col_date].dt.date

    data_df.to_csv(dataPath / data_type / "data.csv", index=False)

    if PUSH_TO_GITHUB:
        content = read_and_encode_file(dataPath / data_type / "data.csv", encode=False)
        try:
            git_file = repo.get_contents(f"data/{data_type}/data.csv")
            repo.update_file(
                git_file.path,
                f"Updated file for {datetime.today().date()}",
                content,
                git_file.sha,
            )
        except Exception as e:
            if isinstance(e, GithubException) and e.status == 404:  # File not found
                repo.create_file(
                    f"data/{data_type}/data.csv",
                    f"Created file for {datetime.today().date()}",
                    content,
                )
            else:
                raise e

        print(f"# {data_type}: Pushed to Github")

    # Push to HuggingFace
    if PUSH_TO_HF:
        hf_repo_id = f'{hf_user}/{data_type.lower()}'
        hf_dataset = Dataset.from_pandas(data_df)
        hf_dataset.push_to_hub(repo_id=hf_repo_id, token=hf_token)
        print(f"# {data_type}: Pushed to Hugging Face")

    if PUSH_TO_GCP:
        csv_buffer = StringIO()
        data_df.to_csv(csv_buffer, index=False)

        # Name of the blob (file) in the bucket
        blob_name = f'{data_type.lower()}.csv'
        blob = bucket.blob(blob_name)

        # Upload CSV directly from the memory buffer
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    print(f"# {data_type}: Updated")

    time.sleep(5)
