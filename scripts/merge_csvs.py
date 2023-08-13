import pandas as pd
import os
import argparse
import hashlib
            
def main():
    parser = argparse.ArgumentParser(
        prog='merge-csvs',
        description='Merge all csv in a folder',
        epilog='Text at the bottom of help')

    parser.add_argument('folder')
    parser.add_argument('filename')

    args = parser.parse_args()

    df = pd.DataFrame([])

    for root, dirs, files in os.walk(args.folder):
        for name in files:
            filename = os.path.join(args.folder, name)
            household_id = hashlib.sha1(filename.encode('utf-8')).hexdigest()
            df_temp = pd.read_csv(filename, parse_dates=['timestamp'])
            df_temp['household_id'] = household_id
            df_temp['timestamp'] = df_temp['timestamp'].apply(lambda dt: dt.replace(day=1, month=1))
            df = pd.concat([df, df_temp], ignore_index=True)

    df = df.set_index('timestamp')
    df.sort_index(inplace=True)
    df.fillna(0, inplace=True)
    df.to_csv(args.filename)


if __name__ == "__main__":
    main()
