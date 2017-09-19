"""Mock Data Generator.

Description:
    Creates mock data for IRSF application.

Usage:
    This python module can be used from the command line with the following aruments:
    > phreak.py <model> <count>

Example:
    > phreak.py cdr 10000 > "/dev/null"
    ... Trying to connect to {DATABASE} Database...
    ... Connected to {DATABASE}.
    ... Inserting {self.count} records into {self.schema}...
    ... Finished inserting {self.count} records.
    ... Done.
"""
import sys
import random
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import psycopg2
from psycopg2.extras import execute_batch

# database connection variables
HOST = os.getenv("")
DATABASE = os.getenv("")
USER = os.getenv("")
PASSWORD = os.getenv("")

# constants
END_DATE = datetime.utcnow()
START_DATE = END_DATE - relativedelta(years=-1)  # one year ago
FRAUD_TYPE = ['iprn', 'pbx', 'sat', 'sim']  # TODO: Icebox
SCHEMA_INFO = None  # TODO: import data.SCHEMA_INFO
TEMP_FILE = "data/temp.csv"
COUNTRY_INFO = None
PHONE_INFO = {"type": {"fix", "mob", "sat"},
              "proba": {0.65, 0.34, 0.01}}


class Phreak(object):
    """Fake data generating class"""

    def __init__(self, count=1000, schema=None):
        self.values = SCHEMA_INFO[schema].values()  # get the field names
        self.schema = schema  # get the table name
        self.count = count  # num records to create

        # set geographic distribution of calls
        self.international = round(count * random.uniform(3.0, 5.0))
        self.emerging = round(self.international * random.uniform(7.0, 11.0))
        self.advanced = round(self.international * random.uniform(37.0, 45.0))
        self.national = self.count - self.international

    # TODO: Icebox
    def insert(self, record):
        """Insert data to database.

        :return: None
        """
        try:
            print(f"Trying to connect to {DATABASE} Database...")
            with psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD) as connection:
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

    @staticmethod
    def random_datetime_generator():
        Y = random.choice(range(2000, 2017))
        m = random.choice(range(1, 13))
        d = random.choice(range(1, 31))
        HH = random.choice(range(0, 24))
        MM = random.choice(range(00, 59))
        SS = random.choice(range(00, 59))
        yield datetime.strptime(f"{Y}-{m}-{d} {HH}:{MM}:{SS}Z")

    @staticmethod
    def random_phonenumber_generator():
        """

        :return A random phonenumber loosley based on NANPA and other countries.:
        """
        cc = random.choice(COUNTRY_INFO.prefix, COUNTRY_INFO.proba).lstrip()
        if cc == 1:
            ic_sn = random.randint(0, 9, 10)
            yield f"+{cc}{ic_sn}"
        else:
            ic_sn = random.randint(0, 9, random.randint(10, (16 - len(cc))))
            yield f"+{cc}{ic_sn}"

    def cdr(self, fraud=False):
        """Call Data Record

        Description:
                   International ~= 3 to 4% of all calls
                Emerging Country ~= 9% of international calls
            Advanced to Emerging ~= 41% of international calls
                        National ~= What is left over from the above
        """
        if fraud:
            self.international = 1 - self.international
            self.emerging = self.advanced
            self.advanced = self.emerging
            self.national = self.count - self.international

        for record in range(self.count):
            record = CDR(provider_id=uuid.uuid4(),
                         date_called=self.random_datetime_generator(),
                         to_country=random.choice(COUNTRY_INFO.name, COUNTRY_INFO.power),
                         to_number=self.random_phonenumber_generator()),
                         to_phone_type=random.choice(PHONE_INFO.type, PHONE_INFO.proba),
                            from_country = mimesis.address.country(),
                            from_number = mimesis.personal.telephone(mask='+###########'),
                            from_phone_type = np.random.choice([]),
                            operator_name = np.random.choice([]),
                            call_duration = np.random.exponential(scale=2.0),
                            call_charge = np.random.exponential(scale=0.5),
                            is_fraud = "False"
                        )

            yield record

        # TODO: Icebox


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


# TODO: Icebox
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


# TODO: Icebox
def pstn(self):
    """Public Switched Telephone Network

    Description:
        Randomly select from 10 to 70 as a percentage of the fraud calls.
    """
    for record in range(self.count):
        pstn = PSTN(date_called=self.random_datetime_generator(),
                    to_country=random.choice(COUNTRY_INFO.name, COUNTRY_INFO.power),
                    to_number=self.random_phonenumber_generator()),
                    to_phone_type = random.choice(PHONE_INFO.type, PHONE_INFO.proba),
                    from_country = mimesis.address.country(),
                    from_number = mimesis.personal.telephone(mask='+###########'),
                    from_phone_type = np.random.choice([]),
                    operator_name = np.random.choice([]),
                    call_duration = np.random.exponential(scale=2.0),
                    call_charge = np.random.exponential(scale=0.5))
        yield pstn

if __name__ == '__main__':
    target_schema = input("Schema: ").lower()
    if target_schema in ['cdr', 'iprn', 'cfcs', 'pstn', 'mno']:
        Phreak(count=int(input("Count: ")), schema=table)
    else:
        print(f"Schema: {table}, not found in the database.")
    print("Exiting...")
    sys.exit(1)
