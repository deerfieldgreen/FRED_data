import pandas as pd
import numpy as np



df_1 = pd.read_csv('data_1.csv', index_col=0)
df_2 = pd.read_csv('data_2.csv', index_col=0)
df = pd.merge(df_1, df_2, how='inner', on='symbol')

df.to_csv('data_v1.csv')
