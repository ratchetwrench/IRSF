"""Call Data Record

Description:
           International ~= 3 to 4% of all calls
        Emerging Country ~= 9% of international calls
    Advanced to Emerging ~= 41% of international calls
                National ~= What is left over from the above

Schema:
           provider_id:                                           # GUID from provider of CDR
           date_called: 2016-11-15T18:49:59Z                      # Datetime.date(start=2015, end=2017)
       to_country_name: China                                     # Address.country_iso(fmt='iso2')
             to_number: +862427825757                             # Personal.telephone(mask=None)
     from_country_name: China                                     # Address.country_iso(fmt='iso2')
           from_number: +9623989253654                            # Personal.telephone(mask=None)
            phone_type: FIX                                       # Personal.telephone(mask=None)
          carrier_name: Verizon - Barbados                        # random.choices(DATA_CDR)
         call_duration: 1.0253015817                              # np.exp()
           call_charge: 0.0512217329                              # np.exp()
              is_fraud: 'False'

    self.national = Pad CDR's with 3 to 4% international calls, to_country != from_country
    self.international = Pad CDR's with 3 to 4% international calls, to_country != from_country
    self.emerging = Pad international calls with ~9% calls from emerging countries to advanced ones, (to_country.mcsi == False) AND (from_country.mcsi == True)

numpy.random.choice(a, size=None, replace=True, p=None)
"""
import os
import inspect
import numpy as np
import random
from mimesis import Generic, Address
import csv
from collections import namedtuple
import psycopg2

# set constants
TEMP_FILE = "temp.csv"
HOST = os.getenv("")
DATABASE = os.getenv("")
USER = os.getenv("")
PASSWORD = os.getenv("")

mimesis = Generic(locale='en')


# self.international = random.randint(2, 5)
# self.emerging = random.randint(7, 11)
# self.advanced = random.randint(37, 45)
# self.national = self.count - self.international
#
# if fraud:
#     self.international = random.randint(92, 95)
#     self.emerging = random.randint(37, 45)
#     self.advanced = random.randint(7, 11)
#     self.national = self.count - self.international


# def insert(count=1000):
#     with psycopg2.connect(DATABASE_URI) as conn:
#         cursor = conn.cursor()
#         try:
#             schema = inspect.stack()[1][3]
#             cursor.execute(f"INSERT INTO {schema} VALUES {schema.__call__()}")
#             conn.commit()
#         except Exception as e:
#             cursor.rollback()
#             print(e)
#
#
def cdr(count=1000, fraud=False):
    CDR = namedtuple("CDR", "provider_id, "
                            "date_called, "
                            "to_country, "
                            "to_number, "
                            "to_phone_type, "
                            "from_country, "
                            "from_number, "
                            "from_phone_type, "
                            "operator_name, "
                            "call_duration, "
                            "call_charge, "
                            "is_fraud")
    if fraud:
        international = random.randint(92, 95)
        emerging = random.randint(37, 45)
        advanced = random.randint(7, 11)
        national = count - international
    else:
        international = random.randint(2, 5)
        emerging = random.randint(7, 11)
        advanced = random.randint(37, 45)
        national = count - international

    record = CDR(provider_id=mimesis.cryptographic.uuid(),
                 date_called=' '.join([mimesis.datetime.date(start=2015, end=2017, fmt='%Y-%m-%d'),
                                       mimesis.datetime.time()]),
                 to_country=mimesis.address.country(),
                 to_number=mimesis.personal.telephone(mask='+###########'),
                 to_phone_type=np.random.choice([]),
                 from_country=mimesis.address.country(),
                 from_number=mimesis.personal.telephone(mask='+###########'),
                 from_phone_type=np.random.choice([]),
                 operator_name=np.random.choice([]),
                 call_duration=np.random.exponential(scale=2.0),
                 call_charge=np.random.exponential(scale=0.5),
                 is_fraud="False")

    yield record


# TODO: helper functions
def _phonenumber():
    """
    given a calling_code
    strip the leading zeros with calling_code.lstrip("0")
    if calling_code == 1
        11 - len(calling_code) generate random integers from 1 to 9
    else
        random(11, 15) - len(calling_code) generate random integers from 1 to 9
        add '+' to front to make E.164 compliant number format
    return phone number

    :return:
    """


with open(TEMP_FILE, 'w') as f:
    for _ in range(10):
        csv.writer(f).writerow(cdr())

        # for _ in range(10):
        #     provider_id=mimesis.uuid(),
        #     date_called=mimesis.date(start=2000, end=2017),
        #     to_country_name=mimesis.country_iso(fmt='iso2'),
        #     to_number=mimesis.telephone(),
        #     from_country_name=mimesis.country_iso(fmt='iso2'),
        #     from_number=mimesis.telephone(),
        #     phone_type=random.choice(['FIX', 'MOB', 'SAT']),
        #     call_duration=np.exp(1.5),
        #     call_charge=np.exp(0.5),
        #     is_fraud='False'
        #
        #     f.write(cdr)

with psycopg2.connect() as conn:
    cursor = conn.cursor()
    try:
        schema = inspect.stack()[1][3]
        cursor.copy_to(TEMP_FILE, schema, sep=',')
        # schema = the function name that called (same name as table
        # schema.__call__() = call the generator function
        # cursor.execute(f"INSERT INTO {schema} VALUES {schema.__call__()}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(e)

# TODO: Create a dictionary of country info.
[
    {
        {"name": "Aruba",
         "power": 0.24,
         "iso": "AW",
         "code": "533",
         "prefix": "297",
         "lat_long": [12.51, -69.96],
         "operator_type": {
             "fix": [{"name": "ATT",
                     "power": .024},
                     {"name": "T-Mobile",
                      "power": .014}
                     ],
             "mob": {...},
             "sat": {...}},
         "bbva": {
             "eagle": "True",
             "nest": "False",
             "other": 'False'}
         },
        ...
    }
]

