import pandas as pd
import os
from dotenv import dotenv_values
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime
import re

ct = datetime.datetime.now().date()


concept_df = pd.read_csv('visit _details/other.tsv',sep='\t')
concept_df = concept_df.convert_dtypes()
columns = ['C_METADATAXML', 'C_COMMENT', 'VALUETYPE_CD', 'M_EXCLUSION_CD', 'C_PATH', 'C_SYMBOL']
concept_df[columns] = concept_df[columns].astype('string')
concept_df.rename(columns=lambda x: x.upper(), inplace=True)
concept_df['UPDATE_DATE'] = ct
concept_df['DOWNLOAD_DATE'] = ct
concept_df['IMPORT_DATE'] = ct
columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']
concept_df[columns] = concept_df[columns].astype('object')
concept_df['C_FACTTABLECOLUMN'].loc[(concept_df['C_FACTTABLECOLUMN'] == 'concept_cd')] = 'visit_fact.concept_cd'

age_df = pd.read_csv('visit _details/ageAtVisit.tsv',sep='\t')
age_df = age_df.convert_dtypes()
columns = ['C_METADATAXML', 'C_COMMENT', 'VALUETYPE_CD', 'M_EXCLUSION_CD', 'C_PATH', 'C_SYMBOL']
age_df[columns] = age_df[columns].astype('string')
age_df.rename(columns=lambda x: x.upper(), inplace=True)
age_df['UPDATE_DATE'] = ct
age_df['DOWNLOAD_DATE'] = ct
age_df['IMPORT_DATE'] = ct
columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']
# df[columns] = df[columns].astype('datetime64[ns]')
age_df[columns] = age_df[columns].astype('object')
age_df['C_FACTTABLECOLUMN'].loc[(age_df['C_FACTTABLECOLUMN'] == 'concept_cd')] = 'visit_fact.concept_cd'

def parse(segment, date_adjust):
    new_dimcode = None
    split = segment[:-1].split(',')[1].split('*')
    date_unit = 'years' if len(split) == 2 else 'months'
    if date_adjust == True:
        new_dimcode = f'(select min(birth_date) + (INTERVAL \'{split[0]} {date_unit}\')  - (INTERVAL \'1 day\') from PATIENT_DIMENSION where patient_num =VISIT_DIMENSION.PATIENT_NUM)' 
    else: # 1 month
        new_dimcode = f'(select min(birth_date) + (INTERVAL \'{split[0]} {date_unit}\') from PATIENT_DIMENSION where patient_num =VISIT_DIMENSION.PATIENT_NUM)' 

    return new_dimcode

for i, row in age_df.iterrows():
    if row['C_TABLENAME'] == 'concept_dimension':
        continue
    dim_code = row['C_DIMCODE']
    substring = re.findall('ADD_MONTHS\(.*?\)', dim_code)
    c_col = 'start_date'
    converted_dim_code = dim_code
    if 'BETWEEN' in dim_code:
        c_op = 'BETWEEN'
        split1 = parse(substring[0], False)
        split2 = parse(substring[1], True)
        converted_dim_code = split1 + ' AND ' + split2
    elif '>=' in dim_code:
        c_op = '>='
        converted_dim_code = parse(substring[0], False)

    age_df.at[i,'C_DIMCODE'] = converted_dim_code
    age_df.at[i,'C_OPERATOR'] = c_op
    age_df.at[i,'C_COLUMNNAME'] = c_col

frames = [concept_df, age_df]
df = pd.concat(frames)




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

metadata = 'ACT_VISIT_DETAILS_V4'

cur = connection.cursor()
try:
    sql = "delete from " + metadata + '\n' \
        "where C_FULLNAME like '%Visit Details%';"
    cur.execute(sql)
       
finally:
    cur.close()
    
    
cur = connection.cursor()
try:
    write_pandas(connection, df, metadata)
finally:
    cur.close()

    
cur = connection.cursor()
try:
    sql = "UPDATE table_access" + '\n' \
        "SET c_facttablecolumn= '" + 'visit_fact' + ".concept_cd'" + "\n" \
        "WHERE C_TABLE_NAME = '" + 'ACT_VISIT_DETAILS_V4' + "';"
    cur.execute(sql)        
finally:
    cur.close()