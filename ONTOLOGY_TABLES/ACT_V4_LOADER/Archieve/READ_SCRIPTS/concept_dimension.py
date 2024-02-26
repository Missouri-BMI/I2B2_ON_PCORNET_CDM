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

table_name = 'CONCEPT_DIMENSION'
# TRUNCATE TABLE
cur = connection.cursor()
cur.execute("TRUNCATE TABLE " + table_name)
cur.close()

# Get all the table_access .tsv files 
directory = 'act_ontology_v4/crc'
files = []
for filename in os.listdir(directory):
    files.append(os.path.join(directory, filename))

for file in files:
    print('reading....' + file)
    df = pd.read_csv(file,sep='\t')
    if not df[df.columns[0]].count():
        continue
    df = df.convert_dtypes()
    df['CONCEPT_BLOB'] = df['CONCEPT_BLOB'].astype('string')
    df['UPLOAD_ID'] = df['UPLOAD_ID'].astype('int64')
    columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']
    df[columns] = df[columns].astype('object')
    df.rename(columns=lambda x: x.upper(), inplace=True)
    df['UPDATE_DATE'] = ct
    df['DOWNLOAD_DATE'] = ct
    df['IMPORT_DATE'] = ct
    df.reset_index()
    write_pandas(connection, df, table_name)
    
# Close connection
connection.close()
