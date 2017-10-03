"""Communications Fraud Control Association Record Generator"""
# -*- coding: utf-8 -*-
import csv
from time import time
import numpy as np
from numpy.random import randint
from random import randrange
import calendar
import pandas as pd


# Recipe inputs
iprn = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/iprn_proba.csv")
cdr_data = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/cdr_data.csv").set_index(["country_name", "prefix"])


records = []


def iprn_country_generator():
    return iprn["country_name"].sample(n=1, replace=True, weights=iprn["iprn_proba"]).values[0]


def writer():
    with open('/Users/davidwrench/Galvanize/irsf/src/data/cfca.csv', 'wb') as f:
        keys = records[0].keys()
        w = csv.DictWriter(f, keys)
        w.writeheader()
        for record in records:
            w.writerow(record)


def datetime_generator():
    Y = np.random.randint(2015, 2017)
    m = np.random.randint(1, 12)
    d = np.random.randint(1, calendar.monthrange(Y, m)[1])
    H = np.random.randint(0, 23)
    M = np.random.randint(0, 59)
    S = np.random.uniform(low=0.0, high=59.0)
    return pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S))


def phonenumber_generator(country_name=None):
    # get known phone numbers 80% of the time, otherwise generate a new one
    try:
        prefix = cdr_data.loc[country_name]["prefix"]
        print(type(prefix))
        print(prefix)
        if prefix == 1:
            sn = randrange(2000000000, 9999999999)
            return "+{}{}".format(prefix, sn)
        else:
            sn = randrange(2000000000, 999999999999)
            return "+{}{}".format(prefix, sn)
    except KeyError:
        prefix = randrange(1, 973)
        sn = randrange(2000000000, 999999999999)
        return "+{}{}".format(prefix, sn)


# Main Functionality
def cfca():
    record = {}
    record["date_added"] = datetime_generator()
    record["phone_number"] = phonenumber_generator(iprn_country_generator())
    records.append(record)


# CDR Generator
def bootstrap():
    record_count = randint(100, 1000)
    print(f"Generating {record_count} CFCA records...")
    for record in range(record_count):
        cfca()
    writer()


if __name__ == '__main__':
    start = time()
    bootstrap()
    print("--- %s seconds ---" % (time() - start))
