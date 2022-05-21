import pandas as pd
import os

filename = os.getcwd() + "/statements/archive21042022.parquet"
df = pd.read_parquet(filename)
print(df)