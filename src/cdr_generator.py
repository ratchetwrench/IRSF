# -*- coding: utf-8 -*-
import csv
from datetime import datetime
import pandas as pd
import numpy as np
import random
from numpy.random import normal
import calendar
from scipy.stats import expon
import time

# Load Data
CDR_SAMPLES = pd.read_csv('/Users/davidwrench/Galvanize/irsf/src/data/cdr_sample.csv',
                          index_col="from_country")
CDR_PROBAS = pd.read_csv('/Users/davidwrench/Galvanize/irsf/src/data/cdr_data.csv',
                         index_col=["country_name", "operator_name"])
ADVANCED_COUNTRIES = CDR_PROBAS[CDR_PROBAS["bbva_proba"].isnull()]
EMERGING_COUNTRIES = CDR_PROBAS.dropna()

# Set distributions to sample from
CALL_CHARGE = expon(loc=.1)
CALL_DURATION = expon(loc=.01)


# Main Functionality
class CDR(object):
    def __init__(self, count=None, fraud=False):

        self.international = int(round(count * normal(loc=0.04, scale=0.01)))
        self.advanced_to_emerging = int(round(self.international * normal(loc=0.41, scale=0.05)))
        self.emerging_to_advanced = int(round(self.international * normal(loc=0.09, scale=0.03)))
        self.national = count - self.international
        self.fraud = fraud
        self.count = count
        self.records = []

        if self.fraud:
            # Override defaults
            self.international = 1 - self.international
            self.advanced_to_emerging = self.emerging_to_advanced  # swapped with self.advanced
            self.emerging_to_advanced = self.advanced_to_emerging  # swapped with self.emerging
            self.national = count - self.international
            CALL_CHARGE = expon(loc=.17)
            CALL_DURATION = expon(loc=.015)

    def writer(self):
        with open('src/data/cdr.csv', 'w') as f:
            keys = self.records[0].keys()
            writer = csv.DictWriter(f, keys)
            writer.writeheader()
            for record in self.records:
                writer.writerow(record)

    def bootstrap(self):
        # National Calls
        for _ in range(self.national):
            self.cdr()

        # International Calls
        for _ in range(self.international):
            self.cdr(international=True)

        # Advanced Market Calls
        for _ in range(self.emerging_to_advanced):
            self.cdr(advanced_to_emerging=True)

        # Emerging Market Calls
        for _ in range(self.advanced_to_emerging):
            self.cdr(emerging_to_advanced=True)

        self.writer()

    @staticmethod
    def datetime_generator():
        # Datetime ranges
        Y = np.random.randint(2015, 2017)
        m = np.random.randint(1, 12)
        d = np.random.randint(1, calendar.monthrange(Y, m)[1])
        H = np.random.randint(0, 24)
        M = np.random.randint(0, 60)
        S = np.random.randint(0, 60)
        # datetime.datetime(year, month, day[, hour[, minute[, second[, microsecond[, tzinfo]]]]])
        return datetime(Y, m, d, H, M, S).isoformat()
        # return "{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S)

    @staticmethod
    def country_generator(emerging_to_advanced=False):
        # df.loc["<index 0 value>, <index 1 value>, ...][<column name>]
        # sample(n=None, frac=None, replace=False, weights=None, random_state=None, axis=None)
        if emerging_to_advanced:
            return np.random.choice(EMERGING_COUNTRIES.index.get_level_values('country_name'))
        else:
            return np.random.choice(ADVANCED_COUNTRIES.index.get_level_values('country_name'))

    @staticmethod
    def operator_generator(country_name=None):
        # df.loc["<index 0 value>, <index 1 value>, ...][<column name>]
        operators = CDR_PROBAS.loc[country_name].index.values
        p = CDR_PROBAS.loc[country_name]["operator_proba"]
        return np.random.choice(operators, p=p)

    @staticmethod
    def phonenumber_generator(country_name=None):
        # get known phone numbers 80% of the time, otherwise generate a new one
        if np.random.random() >= 0.8:
            return np.random.choice(CDR_SAMPLES.loc[country_name]["from_number"], size=1)
        else:
            prefix = CDR_PROBAS.loc[country_name]["prefix"].values[0]
            if prefix == 1:
                sn = random.randrange(2000000000, 9999999999)
                return "+{}{}".format(prefix, sn)
            else:
                sn = random.randrange(2000000000, 999999999999)
                return "+{}{}".format(prefix, sn)

    @staticmethod
    def extension_generator():
        return ''.join(['x', str(random.randrange(10000, 9999999))])

    def cdr(self, international=False, advanced_to_emerging=False, emerging_to_advanced=False):
        """Call Detail Generator

        Description:

        Output:
            extension: 'x3634'
            from_operator: 'Airtel'
            to_number: '+26733098026158'
            from_country: 'Botswana'
            call_duration: [ 1.20537447]
            to_country: 'Botswana'
            from_number: '+267174873925007'
            call_charge: [ 0.57073277]
            to_operator: 'Airtel'
            date_called: '2015-04-18T08:16:02'

        :param international:
        :param advanced_to_emerging:
        :param emerging_to_advanced:
        :return: dictionary of call detail records
        """
        record = {}
        record["call_date"] = self.datetime_generator()
        record["from_country"] = self.country_generator()

        if international:
            record["to_country"] = self.country_generator()
        elif advanced_to_emerging:
            record["to_country"] = self.country_generator()
        elif emerging_to_advanced:
            record["to_country"] = self.country_generator(emerging_to_advanced=True)
        else:
            record["from_number"] = self.phonenumber_generator(country_name=record['from_country'])
            record["from_operator"] = self.operator_generator(country_name=record['from_country'])

        if np.random.random() >= 0.8:
            record["extension"] = self.extension_generator()
        else:
            record["extension"] = None

        record["to_country"] = record['from_country']
        record["to_number"] = self.phonenumber_generator(country_name=record['to_country'])
        record["to_operator"] = self.operator_generator(country_name=record['to_country'])
        record["call_duration"] = CALL_DURATION.rvs(size=1)[0]
        record["call_charge"] = CALL_CHARGE.rvs(size=1)[0]

        return self.records.append(record)


if __name__ == '__main__':
    start = time.time()
    record_count = np.random.randint(100000, 1000000)
    print(f"Generating {record_count} CDR records...")
    x = CDR(count=record_count, fraud=False)
    x.bootstrap()
    stop = time.time()
    run_time = stop - start
    print(f"Created {record_count} records in {run_time} seconds")
    print(f"{record_count / run_time} records per second")
