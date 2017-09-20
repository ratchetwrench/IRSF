
import json
import pandas as pd
from odo import odo


data = pd.read_csv("/Users/davidwrench//Downloads/operator_proba.csv",
                   names=['id','country_name','operator_name','operator_type','probs'])

data.to_json("/Users/davidwrench//Downloads/operator_proba.json", orient='records')

# names=['id', 'country_name','operator_name','operator_type','probs']
#
# odo("/Users/davidwrench//Downloads/operator_proba.csv",
#     "/Users/davidwrench//Downloads/odo_operator_proba.json"
#     )
