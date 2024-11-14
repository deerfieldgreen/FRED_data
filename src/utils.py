
import os
import numpy as np
import pandas as pd
import yaml
from datetime import datetime, timedelta
import base64
from google.cloud import storage
from google.auth import default


def load_config(path):
    with open(path, "r") as f:
        config = yaml.load(f, yaml.FullLoader)
    return config

def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month+1, day=1) - timedelta(days=1)


def del_file(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

def read_and_encode_file(file_path, encode=True):
    """Read the file content and encode it in base64."""
    with open(file_path, 'rb') as file:
        content = file.read()
    if encode:
        return base64.b64encode(content).decode('utf-8')
    else:
        return content

def get_gcp_bucket():
    # Path to the service account key file
    service_account_key_path = 'dfg-analytics-insights-prod-0a3460e5b674.json'
    
    if os.path.exists(service_account_key_path):
        # Use the service account key file if it exists
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(service_account_key_path)
        storage_client = storage.Client()
    else:
        # Use default credentials if the service account key file does not exist
        credentials, project_id = default()
        storage_client = storage.Client(credentials=credentials, project=project_id)

    # Name of your GCP bucket
    bucket_name = 'fred-stlouis'
    bucket = storage_client.bucket(bucket_name)
    return bucket

