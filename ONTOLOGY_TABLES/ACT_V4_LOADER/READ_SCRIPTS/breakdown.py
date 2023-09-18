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
            schema='I2B2DATA'
)
print('Connected...')

table_name = 'QT_BREAKDOWN_PATH'
# TRUNCATE TABLE
connection.cursor().execute("TRUNCATE TABLE " + table_name)

# Get all the table_access .tsv files 
directory = 'act_ontology_v4/breakdowns'
files = []
for filename in os.listdir(directory):
    files.append(os.path.join(directory, filename))

for file in files:
    print('reading....' + file)
    df = pd.read_csv(file,sep='\t')
    df = df.convert_dtypes()
    df['USER_ID'] = df['USER_ID'].astype('string')
    columns = ['CREATE_DATE', 'UPDATE_DATE']
    df[columns] = df[columns].astype('object')
    

    df.rename(columns=lambda x: x.upper(), inplace=True)
    write_pandas(connection, df, table_name)
    
# Close connection
connection.close()
