import pandas as pd
import time
from datetime import datetime
from io import StringIO
import pickle
from fredapi import Fred

def process_non_sofr_data(data_map_dict, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket):
    audit_data = []
    for data_type, data_info in data_map_dict.items():
        if not data_type.startswith('SOFR'):
            data_df = fetch_and_process_data(fred, data_info, col_date)
            save_data(data_df, data_type, dataPath, SAVE_AS_PICKLE)
            audit_data.append(collect_audit_info(data_df, data_type, data_info['data_ref']))
            if PUSH_TO_GCP:
                upload_to_gcp(data_df, data_type, bucket, SAVE_AS_PICKLE)
            print(f"# {data_type}: Updated")
            time.sleep(1)
    return audit_data

def process_sofr_data(sofr_series, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket):
    sofr_data = pd.DataFrame()
    audit_data = []
    for series in sofr_series:
        data_df = fetch_and_process_data(fred, {'data_ref': series}, col_date)
        sofr_data = update_sofr_data(sofr_data, data_df, col_date, series)
        audit_data.append(collect_audit_info(data_df, series, series))
        print(f"# {series}: Updated")
        time.sleep(1)
    
    sofr_data = sofr_data.sort_values(col_date).ffill()
    save_combined_data(sofr_data, dataPath, "combined_sofr_data", SAVE_AS_PICKLE)
    if PUSH_TO_GCP:
        upload_combined_to_gcp(sofr_data, "sofr_data", bucket, SAVE_AS_PICKLE)
    return audit_data

def fetch_and_process_data(fred, data_info, col_date):
    data_ref = data_info['data_ref']
    data_df = fred.get_series(data_ref)
    data_df = data_df.dropna().reset_index()
    data_df.columns = [col_date, data_ref]
    data_df[col_date] = data_df[col_date].dt.date
    return data_df

def save_data(data_df, data_type, dataPath, SAVE_AS_PICKLE):
    csv_dir = dataPath / data_type
    csv_dir.mkdir(parents=True, exist_ok=True)
    data_df.to_csv(csv_dir / "data.csv", index=False)
    if SAVE_AS_PICKLE:
        with open(csv_dir / f"{data_type}.pkl", 'wb') as f:
            pickle.dump(data_df, f)

def collect_audit_info(data_df, series_name, data_ref):
    last_date = data_df[data_df.columns[0]].max()
    last_value = data_df.loc[data_df[data_df.columns[0]] == last_date, data_ref].values[0]
    return {
        "Series Name": series_name,
        "Last Date": last_date,
        "Last Value": last_value,
        "Last Request Datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def upload_to_gcp(data_df, data_type, bucket, SAVE_AS_PICKLE):
    csv_buffer = StringIO()
    data_df.to_csv(csv_buffer, index=False)
    blob_name = f'{data_type.lower()}.csv'
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    if SAVE_AS_PICKLE:
        pickle_buffer = pickle.dumps(data_df)
        pickle_blob_name = f'{data_type.lower()}.pkl'
        pickle_blob = bucket.blob(pickle_blob_name)
        pickle_blob.upload_from_string(pickle_buffer, content_type='application/octet-stream')

def update_sofr_data(sofr_data, new_data, col_date, series):
    if sofr_data.empty:
        return new_data
    else:
        return pd.merge(sofr_data, new_data, on=col_date, how='outer')

def save_combined_data(data, dataPath, filename, SAVE_AS_PICKLE):
    data.to_csv(dataPath / f"{filename}.csv", index=False)
    if SAVE_AS_PICKLE:
        with open(dataPath / f"{filename}.pkl", 'wb') as f:
            pickle.dump(data, f)

def upload_combined_to_gcp(data, filename, bucket, SAVE_AS_PICKLE):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    blob_name = f'{filename}.csv'
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    if SAVE_AS_PICKLE:
        pickle_buffer = pickle.dumps(data)
        pickle_blob_name = f'{filename}.pkl'
        pickle_blob = bucket.blob(pickle_blob_name)
        pickle_blob.upload_from_string(pickle_buffer, content_type='application/octet-stream')