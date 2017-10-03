"""db
__author__ = davidwrench

Created = 10/2/17

Description: # TODO: Description...


Usage: # TODO: Usage...


Example: # TODO: Example...
"""
import sqlalchemy
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine

HOST = "localhost"
DRIVER = "postgres"
PORT = 5432
DATABASE = "irsf"
USER = "postgres"
PASSWORD = "postgres"


def pg():
    engine = create_engine(f'{DRIVER}://{USER}:{DATABASE}@{HOST}:{PORT}')
    connection = engine.connect()
    with connection.begin() as connection:
        return connection
