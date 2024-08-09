import os
import pandas as pd
import json

folder = os.path.abspath("example_data/gem")

split_json = os.path.join(folder, 'split.json')

# get split.json
with open(split_json) as f:
    split = json.load(f)

files = [os.path.join(folder, f + '.csv') for f in split['test_households']]
print(files)


# get the first file for the timestamp column
df = pd.read_csv(os.path.join(folder, files[0]))
df = df.rename(columns={'power': files[0].split('.')[0].split('/')[-1]})
df = df.set_index('timestamp')

# loop through the rest of the files and merge the power column into the dataframe 
for f in files[1:]:
    df2 = pd.read_csv(os.path.join(folder, f))
    df2 = df2.rename(columns={'power': f.split('.')[0].split('/')[-1]})
    df2 = df2.set_index('timestamp')
    df = pd.merge(df, df2, how='outer', left_index=True, right_index=True)

# save the merged dataframe to a csv file
df.to_csv(os.path.join(folder, 'merged.csv'))