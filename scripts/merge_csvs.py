import pandas as pd
import os
import argparse


def main():
    parser = argparse.ArgumentParser(
        prog='merge-csvs',
        description='Merge all csv in a folder',
        epilog='Text at the bottom of help')

    parser.add_argument('folder')
    parser.add_argument('filename')

    args = parser.parse_args()

    os.chdir(args.folder)

    df = pd.DataFrame([])

    for root, dirs, files in os.walk("."):
        for name in files:
            df_temp = pd.read_csv(name)
            df = pd.concat([df, df_temp])

    df.fillna(0, inplace=True)
    df.to_csv(args.filename)


if __name__ == "__main__":
    main()
