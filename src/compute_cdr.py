# coding=utf-8
"""compute_cdr
__author__ = davidwrench

Created = 9/24/17

Description: # TODO: Description...


Usage: # TODO: Usage...


Example: # TODO: Example...
"""
import csv
from dask import dataframe as dd
from dask.distributed import Client
import pandas as pd
from datetime import datetime
import numpy as np
import random
from numpy.random import randint
from numpy.random import normal
from numpy.random import random_sample
import calendar

# Recipe inputs
df = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/cdr_data.csv")

client = Client('127.0.0.1:8786')  # set up local cluster on your laptop


# Helper Functions
def country_generator(country_name=None):
    if country_name:
        return df["country_name"][df["country_name"] != country_name].sample(n=1,
                                                                             replace=True,
                                                                             weights=df[
                                                                                 "country_proba"]).values[
            0]
    else:
        return df["country_name"].sample(n=1, replace=True, weights=df["country_proba"]).values[0]


def emerging_country_generator(country_name=None):
    return df["country_name"].sample(n=1, replace=True, weights=df["bbva_proba"]).values[0]


def operator_generator(country_name=None):
    if country_name:
        return df["operator_name"][df["country_name"] == country_name].sample(n=1,
                                                                              replace=True,
                                                                              weights=df[
                                                                                  "operator_proba"]).values[
            0]
    else:
        return df["operator_name"][df["country_name"] != country_name].sample(n=1,
                                                                              replace=True,
                                                                              weights=df[
                                                                                  "operator_proba"]).values[
            0]


def datetime_generator():
    Y = randint(2015, 2017)
    m = randint(1, 12)
    d = randint(1, calendar.monthrange(Y, m)[1])
    H = randint(0, 23)
    M = randint(0, 59)
    S = np.random.uniform(low=0.0, high=59.0)
    return pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S))


def phonenumber_generator(country_name=None):
    prefix = df["prefix"][df["country_name"] == country_name].unique()[0]
    # TODO: check for len(prefix) == 1
    if prefix == 1:
        # [2–9] for the first digit, and [0–9] all remaining digits
        sn = random.randrange(2000000000, 9999999999)
        return "+{}{}".format(prefix, sn)
    else:
        # [2–9] for the first digit, and [0–9] all remaining digits (11 to 15)
        sn = random.randrange(20000, 999999999)
        return "+{}{}".format(prefix, sn)


# Main Functionality
def cdr():
    record = {}

    record["date_called"] = datetime_generator()
    record["from_country"] = country_generator()
    record["from_number"] = phonenumber_generator(country_name=record['from_country'])
    record["from_operator"] = operator_generator(country_name=record['from_country'])
    record["to_country"] = record['from_country']
    record["to_number"] = phonenumber_generator(country_name=record['to_country'])
    record["to_operator"] = operator_generator(country_name=record['to_country'])
    record["call_duration"] = np.random.exponential(.4)
    record["call_charge"] = np.random.exponential(.015)

    return record


def international_cdr(to_emerging=False, to_advanced=False):
    record = {}

    record["date_called"] = datetime_generator()
    record["from_country"] = country_generator()
    record["from_number"] = phonenumber_generator(country_name=record['from_country'])
    record["from_operator"] = operator_generator(country_name=record['from_country'])
    if to_emerging:
        record["to_country"] = emerging_country_generator(country_name=record['from_country'])
    elif to_advanced:
        record["to_country"] = country_generator(country_name=record['from_country'])
    else:
        record["to_country"] = country_generator()
    record["to_number"] = phonenumber_generator(record['to_country'])
    record["to_operator"] = operator_generator(record['to_country'])
    record["call_duration"] = np.random.exponential(1.)
    record["call_charge"] = np.random.exponential(.1)

    return record


# CDR Generator
def bootstrap(count=1000, fraud=False):
    records = []

    # Set defaults
    print("Generating {} records...".format(count))
    international = int(round(count * normal(loc=0.04, scale=0.01)))
    print("{} International records...".format(international))
    to_emerging = int(round(international * normal(loc=0.41, scale=0.05)))
    print("{} Emerging records...".format(to_emerging))
    to_advanced = int(round(international * normal(loc=0.09, scale=0.03)))
    print("{} Advanced records...".format(to_advanced))
    national = count - international
    print("{} National records...".format(national))

    if fraud:
        # Override defaults
        international = 1 - international
        to_emerging = to_advanced  # swapped with self.advanced
        to_advanced = to_emerging  # swapped with self.emerging
        national = count - international

    # National Calls
    for record in range(national):
        records.append(cdr())

    # International Calls
    for _ in range(international):
        records.append(international_cdr())

    # Advaned Market Calls
    for _ in range(to_advanced):
        records.append(international_cdr(to_advanced=True))

    # Emerging Market Calls
    for _ in range(to_emerging):
        records.append(international_cdr(to_emerging=True))

    return records


# with open('/Users/davidwrench/Galvanize/irsf/src/data/cdr.csv', 'w') as f:
#     records = bootstrap(count=1000, fraud=False)
#     for record in records:
#         f.write(str(record))

    # writer = csv.writer(f, delimiter=' ', quoting=csv.QUOTE_ALL)
    # writer.writerows(bootstrap(count=1000, fraud=False))

pandas_dataframe = pd.DataFrame(bootstrap(count=10000, fraud=False))
pandas_dataframe.to_csv('/Users/davidwrench/Galvanize/irsf/src/data/cdr.csv')
