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
fact_table = 'diagnosis_fact'
tables = ['ACT_ICD9CM_DX_V4', 'ACT_ICD10CM_DX_V4','ACT_ICD10_ICD9_DX_V4']
cur = connection.cursor()
try:
    for table in tables:
        sql = "UPDATE " + table + '\n' \
        "SET c_facttablecolumn= '" + fact_table + ".concept_cd'" + "\n" \
        "WHERE c_dimcode LIKE '%\\Diagnosis\\%' OR c_dimcode LIKE '%\\Diagnoses\\%';"
        cur.execute(sql)
        
        if table == 'ACT_ICD9CM_DX_V4':
            sql = f"""
                update {table}
                set c_visualattributes = 'FH'
                where c_name like '630-677.99 Complications Of Pregnancy, Childbirth, And The Puerperium'"""
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


