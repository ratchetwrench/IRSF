"""constants"""
from collections import namedtuple

SCHEMA_INFO = {"CDR": namedtuple("CDR", "provider_id, "
                                        "date_called, "
                                        "to_country, "
                                        "to_number, "
                                        "from_country, "
                                        "from_number, "
                                        "phone_type, "
                                        "operator_name, "
                                        "call_duration, "
                                        "call_charge, "
                                        "is_fraud"),
               "PSTN": namedtuple("PSTN", "..."),
               "CFCA": namedtuple("CFCS", "..."),
               "IPRN": namedtuple("IPRN", "...")}

PHONE_TYPE = {"fix": .65,
              "mob": .34,
              "sat": .01}
