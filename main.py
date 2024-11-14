import os
import requests
import time
import warnings
from datetime import datetime
from io import StringIO
import pickle

import pandas as pd

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)  # to ensure console display all columns
pd.set_option('display.float_format', '{:0.3f}'.format)
pd.set_option('display.max_row', 50)

from pathlib import Path
from github import Github
from src.utils import get_gcp_bucket
from git import Repo

root_folder = "."
projectPath = Path(rf'{root_folder}')

dataPath = projectPath / 'data'
pickleDataPath = dataPath / 'pickle'
configPath = projectPath / 'config'
credsPath = projectPath / 'credentials'

dataPath.mkdir(parents=True, exist_ok=True)
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
PUSH_TO_GCP = False if os.environ.get("PUSH_TO_GCP") == 'False' else True
SAVE_AS_PICKLE = False if os.environ.get("SAVE_AS_PICKLE") == 'False' else True

##############################################################################
## Settings

config = load_config(path=configPath / "settings.yml")
fred_api_key = os.environ.get("FRED_API_KEY")
fred = Fred(api_key=fred_api_key)

bucket = get_gcp_bucket()
print('Retrieved GCP bucket: {}'.format(bucket))

data_map_dict = config["data_map_dict"]
col_date = config["col_date"]

# TOKENS AND AUTH 
###########################################################################
token = os.environ.get("GIT_TOKEN")
g = Github(token)
repo = g.get_repo(
    "deerfieldgreen/FRED_data"
)  # Replace with your repo details

##############################################################################
## Main

audit_data = []

for data_type in data_map_dict:

    data_ref = data_map_dict[data_type]["data_ref"]
    data_source = data_map_dict[data_type]["data_source"]

    csv_file = dataPath / data_type / "data.csv"

    if data_source == "FRED":
        data_df_new = fred.get_series(data_ref)
        data_df_new = data_df_new.dropna()
        data_df_new = data_df_new.reset_index()
        data_df_new.columns = [col_date, data_ref]
    else:
        continue

    data_df = data_df_new
    data_df.reset_index(drop=True, inplace=True)

    data_df[col_date] = data_df[col_date].dt.date

    # Ensure the directory exists
    csv_dir = dataPath / data_type
    csv_dir.mkdir(parents=True, exist_ok=True)

    data_df.to_csv(csv_dir / "data.csv", index=False)

    # Save as pickle if enabled
    if SAVE_AS_PICKLE:
        pickle_file = csv_dir / f"{data_type}.pkl"
        with open(pickle_file, 'wb') as f:
            pickle.dump(data_df, f)

    # Collect audit information
    last_date = data_df[col_date].max()
    last_value = data_df.loc[data_df[col_date] == last_date, data_ref].values[0]
    audit_data.append({
        "Series Name": data_type,
        "Last Date": last_date,
        "Last Value": last_value,
        "Last Request Datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    if PUSH_TO_GCP:
        csv_buffer = StringIO()
        data_df.to_csv(csv_buffer, index=False)

        # Name of the blob (file) in the bucket
        blob_name = f'{data_type.lower()}.csv'
        blob = bucket.blob(blob_name)

        # Upload CSV directly from the memory buffer
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    print(f"# {data_type}: Updated")

    time.sleep(1)

# Create audit CSV
audit_df = pd.DataFrame(audit_data)
audit_df.to_csv("audit_trail.csv", index=False)

# After the loop, perform a single commit for all changes
if PUSH_TO_GITHUB:
    repo_object = Repo('.')
    remote_url = f"https://{os.getenv('GIT_TOKEN')}@github.com/deerfieldgreen/FRED_data.git"
    # Make sure the remote URL is set correctly
    for remote in repo_object.remotes:
        remote.set_url(remote_url)

    git = repo_object.git
    git.add('--all')
    git.commit('-m', f"Updated Files for {datetime.today()}")
    git.pull('-s','ours')
    git.push()
    print("All changes pushed to GitHub in a single commit.")