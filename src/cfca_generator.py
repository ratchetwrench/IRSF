"""cfca_generator
__author__ = davidwrench

Created = 9/28/17

Description: # TODO: Description...


Usage: # TODO: Usage...


Example: # TODO: Example...
"""
# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
from datetime import datetime
from datetime import timedelta
import numpy as np
import random
from numpy.random import randint
from numpy.random import normal
from numpy.random import random_sample
import calendar

# Recipe inputs
iprn = dataiku.Dataset("iprn").get_dataframe()
df = dataiku.Dataset("cdr_data_prepared").get_dataframe().fillna(0)
cdr = dataiku.Dataset("cdr")
cdr_df = cdr.get_dataframe(columns=['to_country', 'to_number'], sampling='random', ratio=1)

# Helper Functions
iprn_df = df.query('iprn_iprn_proba > 0')


def iprn_country_generator():
    return iprn_df["country_name"].sample(n=1, replace=True, weights=iprn["iprn_proba"]).values[0]


def datetime_generator():
    Y = randint(2015, 2017)
    m = randint(1, 12)
    d = randint(1, (calendar.monthrange(Y, m)[1]))
    H = randint(0, 23)
    M = randint(0, 59)
    S = np.random.uniform(low=0.0, high=59.0)
    return pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S))


def fraud_phonenumber_generator(country_name=None):
    # Return a random number from the created block
    return \
    cdr_df["to_number"][cdr_df["to_country"] == country_name].sample(n=1, replace=True).values[0]


# Main Functionality
def cfca():
    record = {}
    record["date_added"] = datetime_generator()
    record["phone_number"] = fraud_phonenumber_generator(iprn_country_generator())
    return record


# CDR Generator
def bootstrap(count=100):
    records = []
    print("Generating {} records...".format(count))
    for record in range(count):
        records.append(cfca())
    return pd.DataFrame(records)


count = randint(100, 1000)
pandas_dataframe = bootstrap(count=count)

# Recipe outputs
cfca = dataiku.Dataset("cfca")
cfca.write_with_schema(pandas_dataframe)
