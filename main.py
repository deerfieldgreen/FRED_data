import os
import warnings
from datetime import datetime
import pandas as pd
from pathlib import Path
from github import Github
from git import Repo
from fredapi import Fred
import dotenv

from src.utils import load_config, get_gcp_bucket, setup_logging
from src.data_operations import process_data

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:0.3f}'.format)
pd.set_option('display.max_row', 50)

dotenv.load_dotenv(".env")

def main():
    # Setup logging
    logger = setup_logging()

    # Setup paths
    root_folder = "."
    projectPath = Path(rf'{root_folder}')
    dataPath = projectPath / 'data'
    configPath = projectPath / 'config'

    dataPath.mkdir(parents=True, exist_ok=True)
    configPath.mkdir(parents=True, exist_ok=True)

    # Load configuration and setup
    config = load_config(path=configPath / "settings.yml")
    fred_api_key = os.environ.get("FRED_API_KEY")
    fred = Fred(api_key=fred_api_key)

    PUSH_TO_GITHUB = os.environ.get("PUSH_TO_GITHUB") != 'False'
    PUSH_TO_GCP = os.environ.get("PUSH_TO_GCP") != 'False'
    SAVE_AS_PICKLE = os.environ.get("SAVE_AS_PICKLE") != 'False'

    bucket = get_gcp_bucket() if PUSH_TO_GCP else None
    logger.info(f'Retrieved GCP bucket: {bucket}')

    data_map_dict = config["data_map_dict"]
    col_date = config["col_date"]

    # Process data
    audit_data = process_data(data_map_dict, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket)

    # Create audit CSV
    audit_df = pd.DataFrame(audit_data)
    audit_df.to_csv("audit_trail.csv", index=False)

    # Push to GitHub if enabled
    if PUSH_TO_GITHUB:
        push_to_github()

    logger.info("Data processing and updates completed successfully.")

def push_to_github():
    repo_object = Repo('.')
    remote_url = f"https://{os.getenv('GIT_TOKEN')}@github.com/deerfieldgreen/FRED_data.git"
    for remote in repo_object.remotes:
        remote.set_url(remote_url)
    
    git = repo_object.git
    git.add('--all')
    git.commit('-m', f"Updated Files for {datetime.today()}")
    git.pull('-s','ours')
    git.push()
    print("All changes pushed to GitHub in a single commit.")

if __name__ == "__main__":
    main()