"""Communications Fraud Control Association Record Generator"""
# -*- coding: utf-8 -*-
import csv
from datetime import datetime
from time import time
import numpy as np
from numpy.random import randint
import calendar
import pandas as pd

# Recipe inputs
iprn = pd.read_csv("src/data/iprn.csv")
df = pd.read_csv("src/data/cdr_data.csv")
cdr = pd.read_csv("src/data/cdr.csv", usecols=['to_country', 'to_number']).sample(frac=1)
cdr.set_index("country_name", inplace=True)
# Helper Functions
iprn_df = df.query('iprn_iprn_proba > 0')

records = []


def iprn_country_generator():
    return iprn_df["country_name"].sample(n=1, replace=True, weights=iprn["iprn_proba"]).values[0]


def writer():
    with open('src/data/cfca.csv', 'wb') as f:
        keys = records[0].keys()
        w = csv.DictWriter(f, keys)
        w.writeheader()
        for record in records:
            w.writerow(record)


def datetime_generator():
    # Datetime ranges
    y = np.random.randint(2015, 2017)
    m = np.random.randint(1, 12)
    d = np.random.randint(1, calendar.monthrange(y, m)[1])
    h = np.random.randint(0, 24)
    m = np.random.randint(0, 60)
    s = np.random.randint(0, 60)
    return datetime(s, m, d, h, m, s).isoformat()


def fraud_phonenumber_generator(country_name=None):
    # Return a random number from the created block
    return cdr.loc[country_name]["from_number"].sample(n=1, replace=True).values[0]


# Main Functionality
def cfca():
    record = {"date_added": datetime_generator(),
              "phone_number": fraud_phonenumber_generator(iprn_country_generator())}
    records.append(record)


# CDR Generator
def bootstrap(count=100):
    print("Generating {} CFCA records...".format(count))
    for record in range(count):
        cfca()
    writer()


if __name__ == '__main__':
    start = time()
    record_count = randint(100, 1000)
    bootstrap(count=record_count)
    print("--- %s seconds ---" % (time() - start))
