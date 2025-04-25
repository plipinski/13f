import pandas as pd

#cusips = pd.read_csv("cusip8.csv")
#data = pd.read_csv("holdings8.csv")

#cusip_set = set(cusips[0])

# Filter 'data' where 'cupsid' is NOT in the 'cusip' set
#missing = data[~data[0].isin(cusip_set)]

#print(missing)


df = pd.read_csv('all_data.csv')
mask = (df['cik'].isna() | (df['cik'] == '')) & (df['ticker'].isna() | (df['ticker'] == ''))
print(df.loc[mask, 'cupsid'].drop_duplicates())

