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

##
    # NA -> NAC for ACT_VISIT_DETAILS_V4 127 Line
    ##
# Get all the table_access .tsv files 
directory = 'act_ontology_v4/metadata'
files = []
for filename in os.listdir(directory):
    files.append(os.path.join(directory, filename))

for file in files:
    table_name = file.split('/')[-1].split('.')[0]
    print('reading....' + file + ' for ' + table_name)
    # Truncate table_access
    connection.cursor().execute("TRUNCATE TABLE " + table_name)
    df = pd.read_csv(file,sep='\t')
    df = df.convert_dtypes()
    columns = ['C_METADATAXML', 'C_COMMENT', 'VALUETYPE_CD', 'M_EXCLUSION_CD', 'C_PATH', 'C_SYMBOL']
    df[columns] = df[columns].astype('string')
    columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']
    # df[columns] = df[columns].astype('datetime64[ns]')
    df[columns] = df[columns].astype('object')
    df.rename(columns=lambda x: x.upper(), inplace=True)
    
    success, nchunks, nrows, _ = write_pandas(connection, df, table_name)
    
    
# Close connection
connection.close()