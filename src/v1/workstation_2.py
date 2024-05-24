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




# Income statement: date, symbol, revenue, ebitda, ebitdaratio, eps
# 6. get revenue for last available quarter -> Implied AR
# 7. get revenue for last 4 quarter -> AR
print('Downloading income statement data...')


list_3 = []
for symbol in tickers:
    url_IS = f'https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period=quarter&limit={4}&apikey={api_key}'
    response = requests.get(url_IS)
    data = response.json()
    if not data:
        continue
    df = pd.DataFrame(data, index=[0,1,2,3])
    df = df[['date', 'revenue']]
    last_IS_date = df['date'][0]
    implied_AR = df['revenue'][0] * 4
    actual_AR = sum(df['revenue'])
    line = [symbol, last_IS_date, implied_AR, actual_AR]
    list_3.append(line)

df_IS = pd.DataFrame(list_3, columns=['symbol', 'date_last_QIS', 'implied_AR', 'actual_AR'])


# Income Growth: symbol, date, period, calendarYear, growthRevenue
# 8. get annual revenue growth
print('Downloading income growth data...')
list_4 = []
for symbol in tickers:
    url_IG = f'https://financialmodelingprep.com/api/v3/income-statement-growth/{symbol}?period=annual&limit={1}&apikey={api_key}'
    response = requests.get(url_IG)
    data = response.json()
    if not data:
        continue
    df = pd.DataFrame(data, index=[0])
    df = df[['growthRevenue']]
    line = [symbol, df['growthRevenue'][0]]
    list_4.append(line)

df_IG = pd.DataFrame(list_4, columns=['symbol', 'growthRevenue'])

df = pd.merge(df_IG, df_IS, on=['symbol'], how='inner')

df.to_csv('data_2.csv')
