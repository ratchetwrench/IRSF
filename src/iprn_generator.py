"""International Premium Rate Number Generator"""
# -*- coding: utf-8 -*-
import csv
import pandas as pd
from datetime import timedelta, time
import numpy as np
import random
from numpy.random import randint
from numpy.random import normal
import calendar


# Recipe inputs
iprn = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/iprn_data.csv", index_col="country_name").fillna(0)
cdr_data = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/cdr_data.csv", index_col=["country_name"])
cdr = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/cdr.csv", index_col="from_country")
records = []


def writer():
    print(records[0])
    with open('/Users/davidwrench/Galvanize/irsf/src/data/iprn.csv', 'w') as f:
        fieldnames = records[:,0].keys()
        print(type(fieldnames))
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for record in records:
            w.writerow(record)


def iprn_country_generator():
    print(f"Generating IRPN Country...")
    return np.random.choice(cdr_data.index.values)


def iprn_block_generator(country_name):
    print(f"IPRN Block Generator for: {country_name}")
    prefix = np.random.choice(cdr_data.loc[country_name]["prefix"])
    print(f"Using prefix: {prefix} to generate block...")
    sn = random.randrange(2000000000, 9999999999)
    block_range = []
    for _ in range(randint(9, 99)):
        sn += 1
        number_format = "+{}{}".format(prefix, sn)
        block_range.append(number_format)
    print(f"Created: {len(block_range)} phone numbers...")
    return block_range


def emerging_country_generator():
    x = cdr_data.reset_index().drop_duplicates(subset=["country_name", "bbva_proba"])
    choices = x["country_name"]
    p = x["bbva_proba"]
    return np.random.choice(choices, p=p)


def country_generator():
    print(f"Country Generator...")
    x = cdr_data.reset_index().drop_duplicates(subset=["country_name", "country_proba"])
    choices = x["country_name"]
    p = x["country_proba"]
    return np.random.choice(choices, p=p)


def iprn_operator_generator(country_name):
    print(f"Generating IPRN Operator for: {country_name}")
    choices = cdr_data.loc[country_name]["operator_name"]
    p = cdr_data.loc[country_name]["operator_proba"]
    print(f"Operator Probability sum = {p.sum()}")
    if len(choices) == 1:
        return choices
    else:
        return np.random.choice(choices, p=p)


def operator_generator(country_name):
    print(f"Generating Operator for: {country_name}")
    choices = list(cdr_data.loc[country_name]["operator_name"])
    p = cdr_data.loc[country_name]["operator_proba"]
    print(f"Operator Probability sum = {p.sum()}")
    return np.random.choice(choices, p=p)


def datetime_generator():
    Y = randint(2015, 2017)
    m = randint(1, 12)
    d = randint(1, (calendar.monthrange(Y, m)[1]))
    H = randint(0, 23)
    M = randint(0, 59)
    S = np.random.uniform(low=0.0, high=59.0)
    return pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S))


block_range = iprn_block_generator(iprn_country_generator())


def fraud_phonenumber_generator():
    # Return a random number from the created block
    return np.random.choice(block_range)


def target_country_generator():
    print(f"Generating Target Country...")
    x = cdr_data.reset_index().drop_duplicates(subset=["country_name", "country_proba"])
    choices = x["country_name"]
    print(f"Target Country Choices: {len(choices)}")
    p = x["country_proba"]
    print(f"Target Country Probability = {p}")
    return np.random.choice(choices, p=p)


def target_number_generator(country_name):
    return np.random.choice(cdr.loc[country_name]["to_number"])


def extension_generator():
    return ''.join(['x', str(random.randrange(10000, 9999999))])

iprn_country = iprn_country_generator()
iprn_operator = iprn_operator_generator(iprn_country)
target_timefame = datetime_generator()
target_country = target_country_generator()
target_operator = operator_generator(target_country)
target_number = target_number_generator(target_country)


# Main Functionality
def international_cdr(to_emerging=False, to_advanced=False):
    record = {}

    record["date_called"] = target_timefame + timedelta(days=randint(-15, 15))
    record["from_country"] = iprn_country
    record["from_number"] = fraud_phonenumber_generator()
    record["from_operator"] = iprn_operator
    if to_emerging:
        record["to_country"] = emerging_country_generator()
    elif to_advanced:
        record["to_country"] = target_country
    else:
        record["to_country"] = target_country
    record["to_number"] = target_number
    if np.random.random() >= 0.8:
        record["to_extension"] = extension_generator()
    else:
        record["to_extension"] = None
    record["to_operator"] = target_operator
    record["call_duration"] = np.random.exponential(14)
    record["call_charge"] = np.random.exponential(10)
    record["is_fraud"] = "True"

    records.append(record)


# CDR Generator
def bootstrap():
    record_count = randint(1000, 10000)
    # Set defaults
    to_emerging = int(round(record_count * normal(loc=0.09, scale=0.03)))
    to_advanced = int(round(record_count * normal(loc=0.41, scale=0.05)))
    international = record_count - (to_emerging + to_advanced)
    print("Generating {} records...".format(record_count))
    print("{} International records...".format(international))
    print("{} Emerging records...".format(to_emerging))
    print("{} Advanced records...".format(to_advanced))

    # International Calls
    for _ in range(international):
        records.append(international_cdr())

    # Advanced Market Calls
    for _ in range(to_advanced):
        records.append(international_cdr(to_advanced=True))

    # Emerging Market Calls
    for _ in range(to_emerging):
        records.append(international_cdr(to_emerging=True))

    writer()


if __name__ == '__main__':
    start = time()
    bootstrap()
    print("--- %s seconds ---" % (time() - start))
