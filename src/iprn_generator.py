"""International Premium Rate Number Generator"""
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import timedelta
import numpy as np
import random
from numpy.random import randint
from numpy.random import normal
import calendar

# Recipe inputs
iprn = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/iprn_proba.csv").dropna()
cdr_data = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/cdr_data.csv")
cdr = pd.read_csv("/Users/davidwrench/Galvanize/irsf/src/data/cdr.csv")


def iprn_country_generator():
    return iprn["country_name"].sample(n=1, replace=True, weights=iprn["iprn_proba"]).values[0]


def iprn_block_generator(country_name):
    #     iprn_sample = iprn.sample(n=1, replace=True, weights=iprn["iprn_proba"])
    prefix = int(iprn["prefix"][iprn["country_name"] == country_name].sample(n=1, replace=True,
                                                                             weights=iprn[
                                                                                 "iprn_proba"]))
    sn = random.randrange(2000000000, 9999999999)
    # Fill in the back part of the number minus the prefix
    block_range = []
    for _ in range(randint(9, 99)):
        sn += 1
        number_format = "+{}{}".format(prefix, sn)
        block_range.append(number_format)
    return block_range


def emerging_country_generator():
    return df["country_name"].sample(n=1, replace=True, weights=df["bbva_proba"]).values[0]


def country_generator():
    return df["country_name"].sample(n=1, replace=True, weights=df["country_proba"]).values[0]


def iprn_operator_generator(country_name):
    return iprn_df["operator_name"][iprn_df["country_name"] == country_name].sample(n=1,
                                                                                    weights=iprn_df[
                                                                                        "iprn_iprn_proba"],
                                                                                    replace=True).values[
        0]


def operator_generator(country_name):
    return \
    df["operator_name"][df["country_name"] == country_name].sample(n=1, weights=df["country_proba"],
                                                                   replace=True).values[0]


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
    return np.random.choice(block_range, size=1, replace=True, p=None)[0]


def target_country_generator():
    return cdr_data["country_name"].sample(n=1, replace=True, weights=cdr_data["country_proba"]).values[0]


def target_number_generator(country_name):
    return \
    cdr["to_number"][cdr["to_country"] == country_name].sample(n=1, replace=True).values[0]


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
    record["to_operator"] = target_operator
    record["call_duration"] = np.random.exponential(14)
    record["call_charge"] = np.random.exponential(10)
    record["is_fraud"] = "True"

    return record


# CDR Generator
def bootstrap(count=100):
    records = []

    # Set defaults
    to_emerging = int(round(count * normal(loc=0.09, scale=0.03)))
    to_advanced = int(round(count * normal(loc=0.41, scale=0.05)))
    international = count - (to_emerging + to_advanced)
    print("Generating {} records...".format(count))
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

    return pd.DataFrame(records)


count = randint(100, 1000)
pandas_dataframe = bootstrap(count=count)

# Recipe outputs
iprn_fraud = dataiku.Dataset("iprn_fraud")
iprn_fraud.write_with_schema(pandas_dataframe)
