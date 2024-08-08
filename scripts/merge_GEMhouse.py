import os
import pandas as pd

folder = "../example_data/gem"

# get all csv files in the folder
files = [f for f in os.listdir(folder) if f.endswith('.csv')]

# get the first file and read it into a pandas dataframe and rename the power column to the filename
df = pd.read_csv(os.path.join(folder, files[0]))
df = df.rename(columns={'power': files[0].split('.')[0]})
df = df.set_index('timestamp')

# loop through the rest of the files and merge the power column into the dataframe with the timestamp as the index column and rename the power column to the filename
for f in files[1:]:
    df2 = pd.read_csv(os.path.join(folder, f))
    df2 = df2.rename(columns={'power': f.split('.')[0]})
    df2 = df2.set_index('timestamp')
    df = pd.merge(df, df2, how='outer', left_index=True, right_index=True)

# save the merged dataframe to a csv file
df.to_csv(os.path.join(folder, 'merged.csv'))