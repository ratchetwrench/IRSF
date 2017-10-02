# coding: utf-8
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from datetime import datetime
import numpy as np
import random
from numpy.random import randint
from numpy.random import normal
from numpy.random import random_sample
import calendar
from scipy.stats import poisson


# Recipe inputs
cdr = pd.read_csv('~/Downloads/cdr-2.csv', usecols=["from_number", "from_country"])

cdr_df = pd.read_csv('~/Downloads/cdr_data_prepared-2.csv', usecols=["country_name",
                                                                     "country_proba",
                                                                     "bbva_proba",
                                                                     "operator_proba",
                                                                     "operator_name",
                                                                     "prefix"])


# Helper Functions
def country_generator(country_name=None):
    if country_name:
        return cdr_df["country_name"][cdr_df["country_name"] != country_name].sample(n=1,
                                                                                     replace=True,
                                                                                     weights=cdr_df[
                                                                                 "country_proba"]).values[
            0]
    else:
        return cdr_df["country_name"].sample(n=1, replace=True, weights=cdr_df["country_proba"]).values[0]


def emerging_country_generator(country_name=None):
    return cdr_df["country_name"].sample(n=1, replace=True, weights=cdr_df["bbva_proba"]).values[0]
    

def operator_generator(country_name=None):
    if country_name:
        return cdr_df["operator_name"][cdr_df["country_name"] == country_name].sample(n=1,
                                                                                      replace=True,
                                                                                      weights=cdr_df["operator_proba"]).values[0]
    else:
        return cdr_df["operator_name"][cdr_df["country_name"] != country_name].sample(n=1,
                                                                                      replace=True,
                                                                                      weights=cdr_df["operator_proba"]).values[0]


# Helper Functions
CDR_PROBAS = cdr_df[["country_name", "country_proba", "bbva_proba", "operator_proba", "operator_name"]]
CDR_PROBAS = CDR_PROBAS.set_index(["country_name", "operator_name"])
CDR_PROBAS = CDR_PROBAS.fillna(0)

ADVANCED_COUNTRIES = cdr_df["country_name"].dropna().unique()
EMERGING_COUNTRIES = cdr_df["country_name"][cdr_df["bbva_proba"] > 0].unique()
OPERATORS = cdr_df[["country_name", "operator_name", "operator_proba"]].set_index(["country_name"])


def country_generator(emerging=False):
    # df.loc["<index 0 value>, <index 1 value>, ...][<column name>]
    if emerging:
        return np.random.choice(EMERGING_COUNTRIES)
    else:
        return np.random.choice(ADVANCED_COUNTRIES)


def operator_generator(country_name):
    # df.loc["<index 0 value>, <index 1 value>, ...][<column name>]
    operators = OPERATORS.loc[country_name]["operator_name"]
    p = OPERATORS.loc[country_name]["operator_proba"]
    return np.random.choice(operators, p=p)


def datetime_generator():
    Y = randint(2015, 2017)
    m = randint(1, 12)
    d = randint(1, calendar.monthrange(Y, m)[1])
    H = randint(0, 23)
    M = randint(0, 59)
    S = np.random.uniform(low=0.0, high=59.0)
    return pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S))

def to_number_generator(country_name=None):
    if np.random.random() >= 0.15:
        return cdr_df["from_number"][cdr_df["from_country"] == country_name].sample(n=1, replace=True).values[0]
    else:
        prefix = cdr_df["prefix"][cdr_df["country_name"] == country_name].unique()[0]
        if prefix == 1:
            # [2–9] for the first digit, and [0–9] all remaining digits
            sn = random.randrange(2000000000, 9999999999)
            return "+{}{}".format(prefix, sn)
        else:
            # [2–9] for the first digit, and [0–9] all remaining digits (11 to 15)
            sn = random.randrange(20000, 999999999)
            return "+{}{}".format(prefix, sn)


def from_number_generator(country_name=None):
    if np.random.random() >= 0.15:
        return cdr_df["to_number"][cdr_df["to_country"] == country_name].sample(n=1, replace=True).values[0]
    else:
        prefix = cdr_df["prefix"][cdr_df["country_name"] == country_name].unique()[0]
        if prefix == 1:
            # [2–9] for the first digit, and [0–9] all remaining digits
            sn = random.randrange(2000000000, 9999999999)
            return "+{}{}".format(prefix, sn)
        else:
            # [2–9] for the first digit, and [0–9] all remaining digits (11 to 15)
            sn = random.randrange(20000, 999999999)
            return "+{}{}".format(prefix, sn)

def extension_generator():
    if np.random.random() >= 0.8:
        return ''.join(['x', str(random.randrange(10000, 99999))])


expon.pdf(x, loc, scale)
r = expon.rvs(size=1000)


# Main Functionality
def cdr():
    record = {}
    
    record["date_called"] = datetime_generator()
    record["from_country"] = country_generator()
    record["from_number"] = from_number_generator(country_name=record['from_country'])
    record["from_operator"] = operator_generator(country_name=record['from_country'])
    record["to_country"] = record['from_country']
    record["extension"] = extension_generator()
    record["to_number"] = to_number_generator(country_name=record['to_country'])
    record["to_operator"] = operator_generator(country_name=record['to_country'])
    record["call_duration"] = np.random.exponential(scale=.4)
    record["call_charge"] = np.random.exponential(scale=.015)

    return record

def international_cdr(to_emerging=False, to_advanced=False):
    record = {}
        
    record["date_called"] = datetime_generator()
    record["from_country"] = country_generator()
    record["from_number"] = from_number_generator(country_name=record['from_country'])
    record["from_operator"] = operator_generator(country_name=record['from_country'])
    if to_emerging:
        record["to_country"] = emerging_country_generator(country_name=record['from_country'])
    elif to_advanced:
        record["to_country"] = country_generator(country_name=record['from_country'])
    else:
        record["to_country"] = country_generator()
    record["to_number"] = to_number_generator(record['to_country'])
    record["extension"] = extension_generator()
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
    
    return pd.DataFrame(records)


pandas_dataframe = bootstrap(count=1000, fraud=False)


# In[ ]:

pandas_dataframe


# In[ ]:



