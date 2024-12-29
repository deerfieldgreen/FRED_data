import pandas as pd
import time
from datetime import datetime
from io import StringIO
import pickle
from fredapi import Fred

def process_data(data_map_dict, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket):
    audit_data = []
    for data_type, data_info in data_map_dict.items():
        if data_info.get('enabled', True):
            if isinstance(data_info['data_ref'], list):
                audit_data.extend(process_combined_dataset(data_type, data_info, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket))
            else:
                audit_data.extend(process_single_dataset(data_type, data_info, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket))
            print(f"# {data_type}: Updated")
            time.sleep(1)
    return audit_data

def process_single_dataset(data_type, data_info, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket):
    data_df = fetch_and_process_data(fred, data_info, col_date)
    save_data(data_df, data_type, dataPath, SAVE_AS_PICKLE)
    if PUSH_TO_GCP:
        upload_to_gcp(data_df, data_type, bucket, SAVE_AS_PICKLE)
    return [collect_audit_info(data_df, data_type, data_info['data_ref'])]

def process_combined_dataset(data_type, data_info, fred, col_date, dataPath, SAVE_AS_PICKLE, PUSH_TO_GCP, bucket):
    combined_data = pd.DataFrame()
    for series_id in data_info['data_ref']:
        data = fred.get_series(series_id)
        df = pd.DataFrame(data, columns=[series_id])
        df.index.name = col_date
        df.reset_index(inplace=True)
        
        if combined_data.empty:
            combined_data = df
        else:
            combined_data = pd.merge(combined_data, df, on=col_date, how='outer')
    
    combined_data = combined_data.ffill()
    save_data(combined_data, f"{data_type}_combined", dataPath, SAVE_AS_PICKLE)
    if PUSH_TO_GCP:
        upload_to_gcp(combined_data, f"{data_type}_combined", bucket, SAVE_AS_PICKLE)
    
    return [{
        "Series Name": f"{data_type}_Combined",
        "Last Date": combined_data[col_date].max(),
        "Last Value": ", ".join([f"{series}: {combined_data[series].iloc[-1]:.2f}" for series in data_info['data_ref']]),
        "Last Request Datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }]

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