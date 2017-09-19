"""build_data
__author__ = davidwrench

Created = 9/18/17

Description: # TODO: Description...


Usage: # TODO: Usage...


Example: # TODO: Example...
"""
import json
import psycopg2
from psycopg2.extras import RealDictCursor


TEMP_FILE = "temp.csv"
DATASET_SCHEMA = ['country', 'operator']

with psycopg2.connect() as conn:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        for schema in DATASET_SCHEMA:
            cursor.execute("""SELECT * FROM {schema}""")
            with open('', 'w') as f:
                f.json.dumps(cursor.fetchall(), indent=2)
            print(json.dumps(cursor.fetchall(), indent=2))
    except Exception as e:
        conn.rollback()
        print(e)
