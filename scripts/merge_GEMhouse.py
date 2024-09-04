import os
import pandas as pd
import json

folder = os.path.abspath("./example_data/gem")

split_json = os.path.join(folder, 'split.json')

# get split.json
with open(split_json) as f:
    split = json.load(f)

files = [os.path.join(folder, f + '.csv') for f in split['test_households']]

# get the first file for the timestamp column
df = pd.read_csv(os.path.join(folder, files[0]))
# get name of the first file
name = files[0].split('.')[0].split('/')[-1]
df = df.rename(columns={'power': name})
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

df = df.set_index('timestamp')

# loop through the rest of the files and merge the power column into the dataframe 
for f in files[1:]:
    df2 = pd.read_csv(os.path.join(folder, f))
    name = f.split('.')[0].split('/')[-1]
    df2 = df2.rename(columns={'power': name})
    df2['timestamp'] = pd.to_datetime(df2['timestamp'])
    df2['timestamp'] = df2['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df2 = df2.set_index('timestamp')
    df = pd.merge(df, df2, how='outer', left_index=True, right_index=True)

# save the merged dataframe to a csv file
df.to_csv(os.path.join(folder, 'merged.csv'))