from datetime import datetime

import pandas as pd

df = pd.read_csv('data/all_stocks_5yr.csv')

df2 = df[:3]
print(df2)

df2.to_csv('data/testdata.csv')