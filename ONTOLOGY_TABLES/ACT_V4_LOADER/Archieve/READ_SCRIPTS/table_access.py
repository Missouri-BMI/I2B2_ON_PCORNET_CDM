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
            schema='I2B2METADATA'
)
print('Connected...')

table_name = 'TABLE_ACCESS'
# Truncate table_access
connection.cursor().execute("TRUNCATE TABLE " + table_name)

# Get all the table_access .tsv files 
directory = 'act_ontology_v4/table_access'
files = []
for filename in os.listdir(directory):
    files.append(os.path.join(directory, filename))

for file in files:
    print('reading....' + file)
    df = pd.read_csv(file,sep='\t')
    df = df.convert_dtypes()
    columns = ['c_hlevel', 'c_basecode', 'c_metadataxml', 'c_comment', 'c_status_cd', 'valuetype_cd']
    df[columns] = df[columns].astype('string')
    columns = ['c_entry_date', 'c_change_date']
    # df[columns] = df[columns].astype('datetime64[ns]')
    df[columns] = df[columns].astype('object')
    df.rename(columns=lambda x: x.upper(), inplace=True)
    write_pandas(connection, df, table_name)
    
    
# Close the connection
connection.close()