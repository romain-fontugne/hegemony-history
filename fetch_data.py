#! /usr/bin/env python
"""
Fetch hegemony data and compute daily median for selected networks.
"""

from collections import defaultdict
import logging
import arrow
import argparse
import urllib.error
import urllib.request
import json
import os
import csv
import lz4.frame
import pandas as pd
import numpy as np
import matplotlib.pylab as plt

HEGE_LOCAL_URL = 'https://ihr-archive.iijlab.net/ihr/hegemony/ipv4/local/'
HEGE_LOCAL_DIR = 'data/local/'
HEGE_THRESHOLD = 0.1


def load_file(fname):
    logging.warning(f'Loading {fname}')

    pickle_fname = fname+'.pickle'
    df = None

    if not os.path.exists(pickle_fname):
        # Parse file and save data frame to disk
        try:
            with lz4.frame.open(fname, 'r') as f:
                csv_lines = [l.decode('utf-8').rstrip() for l in f]

                df = pd.DataFrame(csv.DictReader(csv_lines))
                df = df.astype({'asn': 'int64', 'originasn': 'int64', 'hege': 'float'})
                df['hege'] = df['hege'].fillna(0.0)

            pd.to_pickle(df, pickle_fname)
        except:
            pass

    else:
        # Load data frame from disk
        df = pd.read_pickle(pickle_fname)

    return df


class HegeHistory:
    def __init__(self, start_date, end_date, asns):
        self.start_date = arrow.get(start_date)
        self.end_date = arrow.get(end_date)
        self.asns = asns

        self.downloaded_files = {}

    def download_all(self, granularity='month'):

        for date in arrow.Arrow.range(granularity, self.start_date, self.end_date):
            fname = f"ihr_hegemony_ipv4_local_{date.year:02d}-{date.month:02d}-{date.day:02d}.csv.lz4"
            date_dir = f"{date.year:02d}/{date.month:02d}/{date.day:02d}"
            url = os.path.join(HEGE_LOCAL_URL, date_dir, fname)
            local_dir = os.path.join(HEGE_LOCAL_DIR, date_dir)
            local_path = os.path.join(local_dir, fname)

            if not os.path.exists(local_path):
                os.makedirs(local_dir, exist_ok=True)
                try:
                    urllib.request.urlretrieve(url, local_path)
                except urllib.error.HTTPError:
                    pass

            if os.path.exists(local_path):
                self.downloaded_files[date] = local_path

        logging.warning(f'Downloaded {len(self.downloaded_files)} files.')

    def plot_median(self):
        med_per_asn = defaultdict(dict)
        for date, fname in self.downloaded_files.items():

            df = load_file(fname)
            if df is None or len(df) == 0:
                continue

            for asn in self.asns:
                df_asn = df.loc[df['originasn'] == asn, ('asn', 'hege')]
                med_per_asn[asn][date] = df_asn.groupby('asn').median()

        for asn, med in med_per_asn.items():
            signals = pd.concat(med, axis=1).T

            asn_to_plot = []
            for a, m in signals.mean().items():
                if m > HEGE_THRESHOLD:
                    asn_to_plot.append(a)

            x = [i[0].datetime for i in signals.index]
            y = signals[asn_to_plot]
            plt.plot(x, y)
            plt.savefig('diffbytes.png')
            plt.title(f"Dependencies of AS{asn}")
            plt.savefig(f"dependencies_AS{asn}.pdf")


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "conf", default="hegemony-history.conf",
        type=argparse.FileType("r"))

    args = parser.parse_args()

    conf = json.load(args.conf)

    start = conf['start']
    end = conf['end']
    asns = conf['asns']

    hh = HegeHistory(start, end, asns)
    hh.download_all()
    hh.plot_median()


if __name__ == "__main__":
    main()
