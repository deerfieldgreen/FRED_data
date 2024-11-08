
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


def reduce_mem_usage(data_df, convert_to_cat=False, verbose=False, col_exclude=[]):
    """ iterate through all the columns of a dataframe and modify the data type
        to reduce memory usage.
    """
    start_mem = data_df.memory_usage().sum() / 1024**2
    if verbose: 
        print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))

    for col in data_df.columns:
        if col in col_exclude:
            continue

        col_type = data_df[col].dtype

        if col_type != object:
            c_min = data_df[col].min()
            c_max = data_df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    data_df[col] = data_df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    data_df[col] = data_df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    data_df[col] = data_df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    data_df[col] = data_df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    data_df[col] = data_df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    data_df[col] = data_df[col].astype(np.float32)
                else:
                    data_df[col] = data_df[col].astype(np.float64)
        else:
            if convert_to_cat:
                data_df[col] = data_df[col].astype('category')

    end_mem = data_df.memory_usage().sum() / 1024**2
    if verbose: 
        print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
        print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))

    return data_df


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
    # for local usage
    # service_account_key_path = projectPath / 'dfg-analytics-insights-prod-0a3460e5b674.json'
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(service_account_key_path)
    # storage_client = storage.Client()

    # for GCP usage
    # Initialize a storage client
    credentials, project_id = default()
    storage_client = storage.Client(credentials=credentials, project=project_id)

    # Name of your GCP bucket
    bucket_name = 'fred-stlouis'
    bucket = storage_client.bucket(bucket_name)
    return bucket

