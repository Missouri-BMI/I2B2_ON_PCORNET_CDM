import pandas as pd
import os
from dotenv import dotenv_values
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime
import random
import math

def write_data(sql):
    cur = connection.cursor()
    try:    
        cur.execute(sql)
    finally:
        cur.close()
        
# read db credential
config = dotenv_values("environment/.env") 

# make connection
print('Connecting to DB...')
connection = snowflake.connector.connect(
            user=config['USER'],
            password=config['PASSWORD'],
            account=config['ACCOUNT'],
            warehouse=config['WAREHOUSE'],
            role=config['ROLE'],
            database=config['DATABASE'],
            schema=config['SCHEMA']
)
print('Connected...')

print('Reading pm_user_data...')
df = pd.read_csv('pm_user_data.csv')
df.rename(columns=lambda x: x.upper(), inplace=True)
table_name = 'PM_USER_DATA'
sql_text = f'truncate table {table_name}'
write_data(sql_text)
write_pandas(connection, df, table_name)
print('Imported pm_user_data...')

print('Reading pm_project_user_roles...')
df = pd.read_csv('pm_project_user_roles.csv')
df.rename(columns=lambda x: x.upper(), inplace=True)
table_name = 'pm_project_user_roles'.upper()
sql_text = f'truncate table {table_name}'
write_data(sql_text)
write_pandas(connection, df, table_name)
print('Imported pm_project_user_roles...')