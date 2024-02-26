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

cur = connection.cursor()
try:
    sql = "UPDATE act_covid_v4" + '\n' \
    "set c_facttablecolumn =  \'covid_fact.concept_cd\'" 
    cur.execute(sql)
finally:
    cur.close()
    
    
cur = connection.cursor()
try:
    sql = "UPDATE table_access" + '\n' \
    "set c_facttablecolumn =  \'covid_fact.concept_cd\'" + '\n' \
    "where c_table_name = 'ACT_COVID_V4';"    
    cur.execute(sql)
finally:
    cur.close()

    
# Close connection
connection.close()
