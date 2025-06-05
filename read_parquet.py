import pandas as pd

# read
df = pd.read_parquet('part-00000-ff8bf157-39b4-409a-86f7-16785be345bd-c000.snappy.parquet')

df.head(5)
