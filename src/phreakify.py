"""Mock Data Generator.

Description:
    Creates and inserts mock data into the Phreak Watch application.

Usage:
    This python module can be used from the command line with the following aruments:
        $ mock_data.py <model> <count>

Example:
    $ mock_data.py cdr 100
    Trying to connect to {DATABASE} Database...
    Connected to {DATABASE}.
    Inserting {self.count} records into {self.schema}...
    Finished inserting {self.count} records.
    Done.


Phreak
    Calls
        count=1 to ~
        from=COUNTRY_OPERATOR_INFO = {'region':
                                        {'sub-region:
                                            {'country':
                                                {'network':
                                                    {'operator':
                                                        {'operator_type': []}
                                                    }
                                                }
                                            }
                                        }
                                      }
        to=COUNTRY_OPERATOR_INFO
        date_range=(now() - 1, now())
        is_fraud='False'

    Fraud(Calls)
        ...
        type=random.choice(FRAUD_PROFILES == {'type':
                                                {'call_duration': np.exp(),
                                                 'call_charges': np.exp()}
                                                }
                                             }
        call_duration=FRAUD_PROFILES.call_duration
        call_charges=FRAUD_PROFILES.call_charges
        is_fraud='True'



"""
import sys
import random
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from mimesis.mimesis import Generic
import os
from data import constants
import psycopg2
from psycopg2.extras import execute_batch

# database connection
HOST = os.getenv("")
DATABASE = os.getenv("")
USER = os.getenv("")
PASSWORD = os.getenv("")

# instance variables
END_DATE = datetime.utcnow()
START_DATE = END_DATE - relativedelta(years=-1)  # one year ago
FRAUD_TYPE = ['iprn', 'pbx', 'sat', 'sim']
SCHEMA_INFO = None  # TODO: import data.SCHEMA_INFO of named tuple for database


class Phreak(object):
    """Fake data generating class"""

    def __init__(self, count=1000, schema=None):
        self.values = constants.SCHEMA_INFO[schema].values()            # get the field names
        self.schema = schema                                            # get the table name
        self.count = count

        # set geographic distribution of calls
        self.international = round(count * random.uniform(3.0, 5.0))
        self.emerging = round(self.international * random.uniform(7.0, 11.0))
        self.advanced = round(self.international * random.uniform(37.0, 45.0))
        self.national = self.count - self.international

        # set mimesis data to be in english
        self.mimesis = Generic('en')

    def _insert(self):
        """Insert data to database.

        :return: None
        """
        try:
            print(f"Trying to connect to {DATABASE} Database...")
            with psycopg2.connect(host=HOST, database=DATABASE, user=USER,
                                  password=PASSWORD) as connection:
                print(f"Connected to {DATABASE}.")
                cursor = connection.cursor()
                try:
                    print(f"Inserting {self.count} records into {self.schema}...")
                    cursor.execute(
                        f"PREPARE stmt AS INSERT INTO {self.schema} VALUES {self.values.keys()}")
                    execute_batch(cursor, "EXECUTE stmt ()", self.values.items())
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

    def cdr(self, fraud=False):
        """Call Data Record

        Description:
                   International ~= 3 to 4% of all calls
                Emerging Country ~= 9% of international calls
            Advanced to Emerging ~= 41% of international calls
                        National ~= What is left over from the above

        Schema:
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
        if fraud:
            self.international = 1 - self.international
            self.emerging = self.advanced
            self.advanced = self.emerging
            self.national = self.count - self.international

    def cfca(self):
        """Communication Fraud Control Association

    Description:

    Schema:
                    cfca_id: '8dbf79f9-af8f-4422-ae27-db3db261fbed'   # GUID
        terminating_country: 'GB'                                     # DATA_IPRN.country_name
               phone_number: '+28475284523'                           # DATA_IPRN.dialing_prefix
                 date_added: '2016-03-27T04:27:11Z'                   # DATA_IPRN.date_added
    """

        for record in range(self.count):
            self.values.update()

        self.cfca_id = uuid.uuid4()
        # TODO: CFCA.country_iso should be a drawn froma unique set of IPRN countries. (Power Rule Distribution)
        self.country_iso = random.choices(IPRN_COUNTRIES.keys(),
                                          weights=IPRN_COUNTRIES['country_weight'],
                                          k=1)
        self.phone_number = self.mimesis.personal.telephone()
        self.e164_phonenumber = ''.join([x for x in self.phone_number if x.isnumeric()])
        self.date_added = ' '.join(
            [self.mimesis.datetime.date(start=2000, end=2017, fmt='%Y-%m-%d'),
             self.mimesis.datetime.time()])
        self.days_since_date_added = (datetime.now() - self.date_added)

    def iprn(self):
        """International Premium Rate Numbers

        Description:
            Given IPRN data take a sample from an IPRN specific distribution:
                Country     Power(Unique(country_name))
                Payout      Norm(Mean(payout_rate))

        Schema:
                        iprn_id: '8dbf79f9-af8f-4422-ae27-db3db261fbed'   # GUID
            terminating_country: 'Albania - Mobile'                       # DATA_IPRN.country_name
                 dialing_prefix: '363 675'                                # DATA_IPRN.dialing_prefix
                     date_added: '2016-03-27T04:27:11Z'                   # DATA_IPRN.date_added
                     payout_usd: 0.0345                                   # DATA_IPRN.payout_rate
        """

    def pstn(self):
        """Public Switched Telephone Network

        Description:
            Telecom providers have there own metrics on possible fraud.

        Schema:
                       pstn_id: '8dbf79f9-af8f-4422-ae27-db3db261fbed'    # Datetime.date(start=2015, end=2017)
                   date_called: 2016-11-15T18:49:59Z                      # Datetime.date(start=2015, end=2017)
               to_country_name: China                                     # Address.country_iso(fmt='iso2')
                     to_number: +862427825757                             # Personal.telephone(mask=None)
             from_country_name: China                                     # Address.country_iso(fmt='iso2')
                   from_number: +9623989253654                            # Personal.telephone(mask=None)
                    phone_type: FIX                                       # Personal.telephone(mask=None)
                  carrier_name: Verizon                                   # random.choices(DATA_PSTN)
                 call_duration: 1.0253015817                              # np.exp()
                   call_charge: 0.0512217329                              # np.exp()
        """


if __name__ == '__main__':
    table = input("Schema: ").upper()
    records = int(input("Count: "))
    if table in ['CDR', 'IPRN', 'CFCA', 'PSTN', 'MNO']:
        Phreak(count=records, schema=table)
    else:
        print(f"Schema: {table}, not found in the database.")
        print("Exiting...")
        sys.exit(1)
