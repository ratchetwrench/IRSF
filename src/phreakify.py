"""Mock Data Generator.

Description:
    Creates mock data for IRSF application.

Usage:
    This python module can be used from the command line with the following aruments:

Example:
    > phreak.py cdr 10000 > "/dev/null"
    ... Trying to connect to {DATABASE} Database...
    ... Connected to {DATABASE}.
    ... Inserting {self.count} records into {self.schema}...
    ... Finished inserting {self.count} records.
    ... Done.
"""
import sys
from numpy.random import randint
from numpy import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import psycopg2
import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

# database connection variables
HOST = os.getenv("")
DATABASE = os.getenv("")
USER = os.getenv("")
PASSWORD = os.getenv("")

# constants
END_DATE = datetime.utcnow()
START_DATE = END_DATE - relativedelta(years=-1)
FRAUD_TYPE = None
SCHEMA = None
TEMP_FILE = "data/temp.csv"
DATA = pd.read_csv("data/data.csv")

df = pd.DataFrame()


def country_generator():
    yield df["country_name"].sample(n=1, replace=True, weights=df["country_proba"])


def emerging_country_generator():
    yield df["country_name"].sample(n=1, replace=True, weights=df["bbva_proba"])


def operator_generator(country):
    yield df["operator_name"].sample(n=1, replace=True, weights=df["operator_proba"])


def datetime_generator():
    return "{}-{}-{} {:02d}:{:02d}:{:02d}".format(randint(2000, 2017 + 1),
                                                  randint(1, 12 + 1),
                                                  randint(1, 31 + 1),
                                                  randint(1, 24 + 1),
                                                  randint(0, 59 + 1),
                                                  randint(0, 59 + 1))


def phonenumber_generator(country):
    prefix = df[df.country_name == country.values[0]]
    if prefix == 1:
        # [2–9] for the first digit, and [0–9] all remaining digits
        sn = np.random.randint(2000000000, 9999999999)
        yield "+{}{}".format(prefix, sn)
    else:
        # [2–9] for the first digit, and [0–9] all remaining digits (11 to 15)
        sn = random.randrange(20000, 999999999)
        yield "+{}{}".format(prefix, sn)


class CDR():
    """Fake data generating class"""
    def __init__(self, count=1000, schema='cdr', fraud=False):
        self.values = SCHEMA[schema]._fields  # get the field names
        self.schema = schema  # get the table name
        self.count = count  # num records to create

        # 'country_name' from DATA, excluding 'self.from_country'
        self.international = round(count * random.randint(3, 5))
        self.call_duration = np.random.poisson(lam=4.0)
        self.call_charge = np.random.poisson(lam=1.5)

        # 'country_name' from DATA, where 'is_emerging' == 'True'
        self.emerging = round(self.international * random.randint(7, 11))

        # 'country_name' from DATA, where 'is_emerging' == 'False'
        self.advanced = self.international - self.emerging

        # one 'country_name' from DATA
        self.national = self.count - self.international

        # default CDR fields
        self.date_called = datetime_generator()
        self.to_country = random.choice()
        self.to_number = phonenumber_generator()
        self.to_operator = random.choice()
        self.to_phone_type = random.choice()
        self.from_country = random.choice()
        self.from_number = random.choice()
        self.advanced = random.choice()
        self.from_operator_name = random.choice()
        self.call_duration = np.random.poisson(lam=1.0)
        self.call_charge = np.random.poisson(lam=0.5)

        if fraud:
            # change initial ratios
            self.international = 1 - self.international
            self.emerging = self.advanced  # swapped with self.advanced
            self.advanced = self.emerging  # swapped with self.emerging
            self.national = self.count - self.international

            # overwrite CDR fields
            self.date_called = datetime_generator()
            self.to_country = random.choice()
            self.to_number = phonenumber_generator()
            self.to_operator = random.choice()
            self.to_phone_type = random.choice()
            self.from_country = random.choice()
            self.from_number = phonenumber_generator()
            self.advanced = random.choice()
            self.from_operator_name = np.random.choice([])
            self.call_duration = np.random.poisson(lam=4.0)
            self.call_charge = np.random.poisson(lam=10.0)

    def bootstrap(self):
        """Insert data to database.

        :return: None
        """
        for record in range(self.count):
            print(CDR())
            try:
                print(f"Trying to connect to {DATABASE} Database...")
                with psycopg2.connect(host=HOST, database=DATABASE, user=USER,
                                      password=PASSWORD) as connection:
                    print(f"Connected to {DATABASE}.")
                    cursor = connection.cursor()
                    try:
                        print(f"Inserting {self.count} records into {self.schema}...")
                        cursor.execute(
                            f"PREPARE stmt AS INSERT INTO {self.schema} VALUES {record._fields}")
                        execute_batch(cursor, "EXECUTE stmt ()", record)
                        cursor.execute("DEALLOCATE stmt")
                        print(f"Finished inserting {self.count} records.")
                        cursor.commit()
                    except Exception as e:
                        print(f"Failed to insert into {self.schema}.")
                        print(f"Rolling back commits...")
                        cursor.rollback()
                        print(e)
            except Exception as e:
                print(f"Failed to connect to {DATABASE}.")
                print(e)
            finally:
                print("Done.")


def pstn():
    """Public Switched Telephone Network

    Description:
        Randomly select from 10 to 70 as a percentage of the fraud calls.
    """
    pass


def cfca():
    """Communication Fraud COntrol Association"""
    pass


if __name__ == '__main__':
    target_schema = input("Schema [cdr]: ")
    target_count = int(input("Count [1000]: "))
    if target_schema.lower() in ['cdr', 'iprn', 'cfca', 'pstn', 'mno']:
        CDR(count=target_count, schema=target_schema)
    else:
        print(f"Schema: {target_schema}, not found in the database.")
        print("Exiting...")
        sys.exit(1)
