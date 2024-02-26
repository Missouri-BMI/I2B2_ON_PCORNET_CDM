import pandas as pd
import os
from dotenv import dotenv_values
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

"""
db credential
"""
# read db credential
config = dotenv_values("../environment/.env") 

# make connection
print('Connecting to DB...')
connection = snowflake.connector.connect(
            user=config['USERNAME'],
            password=config['PASSWORD'],
            account=config['ACCOUNT'],
            warehouse=config['WAREHOUSE'],
            role=config['ROLE'],
            database=config['DATABASE'],
            schema=config['SCHEMA']
)
print('Connected...')

# modify the ontology tables
fact_table = 'prescribing_fact'
tables = ['ACT_MED_ALPHA_V4', 'ACT_MED_VA_V4']
cur = connection.cursor()
try:
    for table in tables:
        sql = "UPDATE " + table + '\n' \
        "SET c_facttablecolumn= '" + fact_table + ".concept_cd'" + "\n" \
        "WHERE c_dimcode LIKE '%\\Medications\\%';"
        cur.execute(sql)
finally:
    cur.close()

cur = connection.cursor()
try:
    for table in tables:
        sql = "UPDATE table_access" + '\n' \
        "SET c_facttablecolumn= '" + fact_table + ".concept_cd'" + "\n" \
        "WHERE C_TABLE_NAME = '" + table + "';"
        cur.execute(sql)
finally:
    cur.close()
    
# Close connection
connection.close()