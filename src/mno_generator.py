"""mno_generator
__author__ = davidwrench

Created = 9/30/17

Description: # TODO: Description...


Usage: # TODO: Usage...


Example: # TODO: Example...
"""
# -*- coding: utf-8 -*-
import csv
import pandas as pd
import numpy as np
import calendar
import time
from scipy.stats import expon

df = pd.read_csv('/Users/davidwrench/Galvanize/irsf/src/data/cdr.csv',
                 usecols=['from_number', 'from_operator'])

# Constant device tenures
ANNUAL = expon(loc=.1)
BI_ANNUAL = expon(loc=.2)
EVENTUAL = expon(loc=.4)

PHONE_TENURE = expon(loc=.05)
DEVICE_TENURE = expon(loc=.1)
SIM_TENURE = expon(loc=.05)
MNO_TENURE = expon(loc=.05)

records = []


def writer():
    with open('/Users/davidwrench/Galvanize/irsf/src/data/mno.csv', 'a') as f:
        keys = records[0].keys()
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def datetime_generator():
    Y = np.random.randint(2015, 2017)
    m = np.random.randint(1, 12)
    d = np.random.randint(1, calendar.monthrange(Y, m)[1])
    H = np.random.randint(0, 23)
    M = np.random.randint(0, 59)
    S = np.random.uniform(low=0.0, high=59.0)
    return pd.to_datetime("{}-{}-{} {:02d}:{:02d}:{:4f}".format(Y, m, d, H, M, S))


# Main Functionality
def mno(fraud=False):
    """Mobile Network Operators

    Schema:
        mobile_phone_number,
        mobile_country_code,
        payfone_tenure,
        phone_number_tenure,
        phone_number_velocity,
        mno_name,
        mno_tenure,
        mno_velocity,
        device_tenure,
        device_velocity,
        sim_tenure,
        sim_velocity

    Description:
        Tenure is in days
        Velecity is count of events
    """
    record = {}

    record["event_date"] = datetime_generator()
    record["mobile_phone_number"] = df["from_number"].sample(n=1).values[0]

    # Number fraud
    if fraud == 'number':
        record["phone_number_tenure_days"] = PHONE_TENURE.rvs(loc=.005) * 365
    else:
        record["phone_number_tenure_days"] = PHONE_TENURE.rvs() * 365
    record["phone_number_velocity"] = 90 / record["phone_number_tenure_days"]

    # Device fraud
    if fraud == 'device':
        record["device_tenure_days"] = DEVICE_TENURE.rvs(loc=.01) * 365
    else:
        record["device_tenure_days"] = np.random.choice(
            [ANNUAL.rvs(), BI_ANNUAL.rvs(), EVENTUAL.rvs()])
    record["device_velocity"] = 90 / record["device_tenure_days"]

    # SIM fraud
    if fraud == 'sim':
        record["sim_tenure_days"] = SIM_TENURE.rvs(loc=.1) * 365
    else:
        record["sim_tenure_days"] = SIM_TENURE.rvs() * 365

    record["sim_velocity"] = 90 / record["sim_tenure_days"]
    record["mno_name"] = df["from_operator"].sample(n=1).values[0]
    record["mno_tenure_days"] = MNO_TENURE.rvs() * 365

    if fraud:
        record["is_fraud"] = "True"
    else:
        record["is_fraud"] = "False"

    records.append(record)


def bootstrap(count=100):
    print("Generating {} Records...".format(count))
    fraud_count = int(round(count * np.random.uniform(0.01, 0.05)))
    print("{} Non-Fraud Records...".format(count - fraud_count))
    print("{} Fraud Records...".format(fraud_count))
    for record in range(count - fraud_count):
        mno(fraud=False)

    for record in range(fraud_count):
        mno(fraud=True)

    writer()


if __name__ == '__main__':
    start = time.time()
    record_count = np.random.randint(10000, 100000)
    bootstrap(count=record_count)
    print("--- %s seconds ---" % (time.time() - start))
