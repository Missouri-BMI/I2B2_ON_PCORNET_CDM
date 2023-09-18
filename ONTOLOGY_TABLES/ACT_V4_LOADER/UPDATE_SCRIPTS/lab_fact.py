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
fact_table = 'lab_fact'
tables = ['ACT_LOINC_LAB_PROV_V4', 'ACT_LOINC_LAB_V4']
cur = connection.cursor()
try:
    for table in tables:
        sql = "UPDATE " + table + '\n' \
        "SET c_facttablecolumn= '" + fact_table + ".concept_cd'" + "\n" \
        "WHERE c_dimcode LIKE '%\\Lab\\%' OR c_dimcode LIKE '%\\Labs\\%';"
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