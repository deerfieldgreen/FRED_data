import json
import logging
import requests
import pandas as pd
import datetime as dt
import os
from pathlib import Path
from github import Github
import configparser
import io
tickers = list(pd.read_csv('tickers.csv')['tickers'])
api_key = 'fZHHrqRjKy6l29UO6lHcwpqMD5GOBvj2'


# Daily Chart EOD: date, close
# 1. get the end price for 2024-03-31
# 2. get the end price for 2023-12-31 -> 3-month change
# 3. get the end price for 2023-03-31 -> 1-year change
start_date = '2023-03-01'
end_date = '2024-03-31'
date_3months_ago = '2023-12-31'
date_1year_ago = '2023-03-31'

print('Downloading Daily Chart data...')

list_1 = []
tickers_excluded = {}
for symbol in tickers:
    url_DC = f'https://financialmodelingprep.com/api/v3//historical-price-full/{symbol}?from={start_date}&to={end_date}&apikey={api_key}'
    response = requests.get(url_DC)
    if not response.json():
        tickers_excluded[symbol] = f"{symbol} no data available"
        continue

    data = response.json()["historical"]
    df = pd.DataFrame(data)
    df = df[['date', 'close']]
    date = max(df['date'])

    if not any(df['date'] <= date_3months_ago):
        tickers_excluded[symbol] = f"{symbol} no data before {date_3months_ago}"

        continue
    date_3M = max(df[df['date'] <= date_3months_ago]['date'])

    if not any(df['date'] <= date_1year_ago):
        tickers_excluded[symbol] = f"{symbol} no data before {date_1year_ago}"
        continue
    date_1Y = max(df[df['date'] <= date_1year_ago]['date'])

    S = list(df[df['date'] == date]['close'])[0]
    r_3M = (list(df[df['date'] == date]['close'])[0] - list(df[df['date'] == date_3M]['close'])[0]) / list(df[df['date'] == date_3M]['close'])[0]
    r_1Y = (list(df[df['date'] == date]['close'])[0] - list(df[df['date'] == date_1Y]['close'])[0]) / list(df[df['date'] == date_1Y]['close'])[0]
    line = [symbol, S, r_3M, r_1Y]
    list_1.append(line)

df_DC = pd.DataFrame(list_1, columns=['symbol', 'previous_close', 'r_3M', 'r_1Y'])




# Statement Analysis: symbol, date, period, marketCap, enterpriseValue
# 4. get market cap
# 5. get enterprise value
print('Downloading statement analysis data...')

list_2 = []
empty_list_2 = []

for symbol in tickers:
    url_SA = f'https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period=quarter&limit={1}&apikey={api_key}'
    response = requests.get(url_SA)
    data = response.json()
    df = pd.DataFrame(data)
    df = df[["enterpriseValue", "marketCap"]]
    line = [symbol, df['enterpriseValue'][0], df['marketCap'][0]]
    list_2.append(line)

df_SA = pd.DataFrame(list_2, columns=['symbol', 'enterpriseValue', 'marketCap'])


df = pd.merge(df_DC, df_SA, on=['symbol'], how='inner')


df.to_csv('data_1.csv')
print('Here are the tickers not included:')
for key, value in tickers_excluded.items():
    print(f'{key}: {value}')

if __name__ == '__main__':
    pass
