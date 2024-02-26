import pandas as pd
import os
from dotenv import dotenv_values
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime
  
# current timestamp
ct = datetime.datetime.now()


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
            schema='I2B2DATA'
)
print('Connected...')

# 
dimension_table = 'CONCEPT_DIMENSION'

# clear demographic concepts
cur = connection.cursor()
try:
    sql = "delete from " + dimension_table + '\n' \
        "where concept_path like '%Demographics%';"
    cur.execute(sql)
       
finally:
    cur.close()
    
# map pcornet concepts
df = pd.read_csv('concept_dimension/pcornet-concepts.tsv',sep='\t')
df = df.convert_dtypes()


df['CONCEPT_BLOB'] = df['CONCEPT_BLOB'].astype('string')
columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']
df[columns] = df[columns].astype('object')
df['UPLOAD_ID'] = pd.to_numeric(df['UPLOAD_ID'], errors='coerce').astype('Int64')
df['UPDATE_DATE'] = ct
df['DOWNLOAD_DATE'] = ct
df['IMPORT_DATE'] = ct

# write new concepts
cur = connection.cursor()
try:
    write_pandas(connection, df, dimension_table)
finally:
    cur.close()