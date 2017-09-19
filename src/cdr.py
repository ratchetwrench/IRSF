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
import numpy as np
import random
import csv
from collections import namedtuple


TEMP_FILE = "temp.csv"
HOST = os.getenv("")
DATABASE = os.getenv("")
USER = os.getenv("")
PASSWORD = os.getenv("")

COUNTRY_INFO = ['USA', 'GB', 'RU', 'CN']
PARETO_SHAPE = 1.


prob = np.random.pareto(PARETO_SHAPE, len(COUNTRY_INFO))
prob /= np.sum(prob)

print(np.random.choice(COUNTRY_INFO, p=prob))


out_list = []
for p, country in zip(prob, COUNTRY_INFO):
    out_list.append({
        'name': country,
        'power': p,
    })
print(out_list)


# def cdr(count=1000, fraud=False):
#     CDR = namedtuple("CDR", "to_phone_type, "
#                             "from_phone_type, "
#                             "operator_name, "
#                             "call_duration, "
#                             "call_charge, "
#                             "is_fraud")
#     if fraud:
#         international = random.randint(92, 95)
#         emerging = random.randint(37, 45)
#         advanced = random.randint(7, 11)
#         national = count - international
#     else:
#         international = random.randint(2, 5)
#         emerging = random.randint(7, 11)
#         advanced = random.randint(37, 45)
#         national = count - international
#
#     record = CDR(to_phone_type=np.random.choice(COUNTRY_INFO, p=prob),
#                  from_phone_type=np.random.choice([]),
#                  operator_name=np.random.choice([]),
#                  call_duration=np.random.exponential(scale=2.0),
#                  call_charge=np.random.exponential(scale=0.5),
#                  is_fraud="False")
#
#     yield record


# with open(TEMP_FILE, 'w') as f:
#     for _ in range(10):
#         csv.writer(f).writerow(cdr())
