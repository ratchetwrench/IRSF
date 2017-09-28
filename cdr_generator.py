# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import random
from numpy.random import normal
import calendar


# Load Data
cdr = pd.read_csv('~/Downloads/cdr-2.csv', usecols=["from_number", "from_country"])

cdr_df = pd.read_csv('~/Downloads/cdr_data_prepared-2.csv', usecols=["country_name",
                                                                     "country_proba",
                                                                     "bbva_proba",
                                                                     "operator_proba",
                                                                     "operator_name",
                                                                     "prefix"])


# Constants
CDR_DATA = cdr_df.set_index("country_name")
CDR_PROBAS = cdr_df.set_index(["country_name", "operator_name"]).fillna(0)
CDR_SAMPLES = cdr.set_index("from_country").drop_duplicates(inplace=True)

ADVANCED_COUNTRIES = cdr_df["country_name"].values
EMERGING_COUNTRIES = cdr_df["country_name"][cdr_df["bbva_proba"] > 0].values
OPERATORS = cdr_df[["country_name", "operator_name", "operator_proba"]].set_index(["country_name"])

YEARS = np.array([2015, 2017])
MONTHS = np.arange(1, 12 + 1)
HOURS = np.arange(1, 24 + 1)
MINUTES = np.arange(0, 59 + 1)


# Main Functionality
class CDR(object):
    def __init__(self, count=None, fraud=False):

        self.international = int(round(count * normal(loc=0.04, scale=0.01)))
        self.advanced_to_emerging = int(round(self.international * normal(loc=0.41, scale=0.05)))
        self.emerging_to_advanced = int(round(self.international * normal(loc=0.09, scale=0.03)))
        self.national = count - self.international
        self.fraud = fraud
        self.count = count
        self.records = {}

        if self.fraud:
            # Override defaults
            self.international = 1 - self.international
            self.advanced_to_emerging = self.emerging_to_advanced  # swapped with self.advanced
            self.emerging_to_advanced = self.advanced_to_emerging  # swapped with self.emerging
            self.national = count - self.international

        # National Calls
        for record in range(self.national):
            self.cdr()

        # International Calls
        for _ in range(self.international):
            self.cdr(international=True)

        # Advaned Market Calls
        for _ in range(self.emerging_to_advanced):
            self.cdr(advanced_to_emerging=True)

        # Emerging Market Calls
        for _ in range(self.advanced_to_emerging):
            self.cdr(emerging_to_advanced=True)

    @staticmethod
    def datetime_generator():
        Y = np.random.choice([2015, 2017])
        m = np.random.choice(MONTHS)
        d = np.random.choice(1, calendar.monthrange(Y, m)[1])
        H = np.random.choice(HOURS)
        M = np.random.choice(MINUTES)
        S = np.random.uniform(low=0.0, high=59.0)
        return "{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S)

    @staticmethod
    def country_generator(emerging_to_advanced=False):
        # df.loc["<index 0 value>, <index 1 value>, ...][<column name>]
        if emerging_to_advanced:
            return np.random.choice(EMERGING_COUNTRIES)
        else:
            return np.random.choice(ADVANCED_COUNTRIES)

    @staticmethod
    def operator_generator(country_name=None):
        # df.loc["<index 0 value>, <index 1 value>, ...][<column name>]
        operators = OPERATORS.loc[country_name]["operator_name"]
        p = OPERATORS.loc[country_name]["operator_proba"]
        return np.random.choice(operators, p=p)

    @staticmethod
    def phonenumber_generator(country_name=None):

        # get known phone numbers 80% of the time, otherwise generate a new one
        if np.random.random() >= 0.8:
            return np.random.choice(CDR_SAMPLES.loc[country_name]["from_number"], size=1)
        else:
            prefix = CDR_DATA.loc[country_name]["prefix"]
            if prefix == 1:
                sn = random.randrange(2000000000, 9999999999)
                return "+{}{}".format(prefix, sn)
            else:
                sn = random.randrange(2000000, 999999999)
                return "+{}{}".format(prefix, sn)

    @staticmethod
    def extension_generator():
        return ''.join(['x', str(random.randrange(10000, 9999999))])

    def cdr(self, international=False, advanced_to_emerging=False, emerging_to_advanced=False):
        record = {}
        self.records["date_called"] = self.datetime_generator()
        self.records["from_country"] = self.country_generator()

        if international:
            record["to_country"] = self.country_generator()
        elif advanced_to_emerging:
            record["to_country"] = self.country_generator()
        elif emerging_to_advanced:
            record["to_country"] = self.country_generator(emerging_to_advanced=True)
        else:
            record["from_number"] = self.phonenumber_generator(country_name=['from_country'])
            record["from_operator"] = self.operator_generator(country_name=record['from_country'])

        if np.random.random() >= 0.8:
            record["extension"] = self.extension_generator()
        else:
            record["extension"] = None

        record["to_country"] = record['from_country']
        record["to_number"] = self.phonenumber_generator(country_name=record['to_country'])
        record["to_operator"] = self.operator_generator(country_name=record['to_country'])

        if self.fraud:
            record["call_duration"] = np.random.exponential(scale=.4)
            record["call_charge"] = np.random.exponential(scale=.015)
        else:
            record["call_duration"] = np.random.exponential(scale=.4)
            record["call_charge"] = np.random.exponential(scale=.015)

        return self.records.update(record)


if __name__ == '__main__':
    x = CDR(count=100, fraud=False)
    print(x)
