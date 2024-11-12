#! /usr/bin/env python
"""
Fetch hegemony data and compute daily median for selected networks.
"""

from collections import defaultdict
import arrow
import argparse
import urllib.request
import json
import os
import csv
import shutil
import lz4.frame
import pandas as pd
import matplotlib.pylab as plt

HEGE_LOCAL_URL = 'https://ihr-archive.iijlab.net/ihr/hegemony/ipv4/local/'
HEGE_LOCAL_DIR = 'data/local/'


class HegeHistory:
    def __init__(self, start_date, end_date):
        self.start_date = arrow.get(start_date)
        self.end_date = arrow.get(end_date)

        self.data = {}

    def download_all(self, granularity='month'):

        for date in arrow.Arrow.range(granularity, self.start_date, self.end_date):
            fname = f"ihr_hegemony_ipv4_local_{date.year:02d}-{date.month:02d}-{date.day:02d}.csv.lz4"
            date_dir = f"{date.year:02d}/{date.month:02d}/{date.day:02d}"
            url = os.path.join(HEGE_LOCAL_URL, date_dir, fname)
            local_dir = os.path.join(HEGE_LOCAL_DIR, date_dir)
            local_path = os.path.join(local_dir, fname)

            if not os.path.exists(local_path):
                os.makedirs(local_dir)
                urllib.request.urlretrieve(url, local_path)

            if os.path.exists(local_path):
                pickle_fname = local_path+'.pickle'
                if not os.path.exists(pickle_fname):
                    # Parse file and save data frame to disk
                    with lz4.frame.open(local_path, 'r') as f:
                        csv_lines = [l.decode('utf-8').rstrip() for l in f]

                        df = pd.DataFrame(csv.DictReader(csv_lines))
                        df = df.astype({'asn': 'int64', 'originasn': 'int64', 'hege': 'float'})
                        df['hege'] = df['hege'].fillna(0.0)
                        self.data[date] = df

                        pd.to_pickle(df, pickle_fname)

                else:
                    # Load data frame from disk
                    df = pd.read_pickle(pickle_fname)
                    self.data[date] = df

    def compute_median(self):
        for asn in [3269, 3215, 4713, 2497, 9198, 35168, 206026, 48503]:
            med = {}
            for date, df in self.data.items():
                df = df[df['originasn'] == 2497][['asn', 'hege']]
                med[date] = df.groupby('asn').median()


            signals = pd.concat(med, axis=1).T
            signals.plot()
            plt.title(f"Dependencies of AS{asn}")
            plt.savefig(f"dependencies_AS{asn}.pdf")



def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "conf", default="hegemony-history.conf",
        type=argparse.FileType("r"))

    args = parser.parse_args()

    conf = json.load(args.conf)

    start = "2021-06-01"
    end = "2024-10-01"

    hh = HegeHistory(start, end)
    hh.download_all()
    hh.compute_median()
    # plot_median()

if __name__ == "__main__":
    main()
