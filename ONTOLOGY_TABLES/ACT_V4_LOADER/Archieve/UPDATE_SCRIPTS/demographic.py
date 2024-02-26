import pandas as pd
import os
from dotenv import dotenv_values
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime

ct = datetime.datetime.now().date()

"""
other ontology update for ACT-SNOWFLAKE
"""
concept_df = pd.read_csv('demographics/others.tsv',sep='\t')
concept_df = concept_df.convert_dtypes()
columns = ['C_METADATAXML', 'C_COMMENT', 'VALUETYPE_CD', 'M_EXCLUSION_CD', 'C_PATH', 'C_SYMBOL']
concept_df[columns] = concept_df[columns].astype('string')
concept_df.rename(columns=lambda x: x.upper(), inplace=True)
concept_df['UPDATE_DATE'] = ct
concept_df['DOWNLOAD_DATE'] = ct
concept_df['IMPORT_DATE'] = ct
columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']

# df[columns] = df[columns].astype('datetime64[ns]')
concept_df[columns] = concept_df[columns].astype('object')
#
concept_df['C_FACTTABLECOLUMN'].loc[(concept_df['C_FACTTABLECOLUMN'] == 'concept_cd')] = 'demographic_fact.concept_cd'
#
concept_df['C_FACTTABLECOLUMN'].loc[(concept_df['C_FULLNAME'] == '\ACT\Demographics\Patient Counts\One Medication\\')] = 'PRESCRIBING_FACT.concept_cd'
#
concept_df['C_FACTTABLECOLUMN'].loc[(concept_df['C_FULLNAME'] == '\ACT\Demographics\Patient Counts\One Procedure\\')] = 'PROCEDURE_FACT.concept_cd'
#
concept_df['C_FACTTABLECOLUMN'].loc[(concept_df['C_FULLNAME'] == '\ACT\Demographics\Patient Counts\One Lab\\')] = 'LAB_FACT.concept_cd'
#
concept_df['C_FACTTABLECOLUMN'].loc[(concept_df['C_FULLNAME'] == '\ACT\Demographics\Patient Counts\One Diagnosis\\')] = 'DIAGNOSIS_FACT.concept_cd'

"""
GPC-Site ontology update for ACT-SNOWFLAKE
"""
gpc_concept_df = pd.read_csv('demographics/gpc-sites.tsv',sep='\t')
gpc_concept_df = gpc_concept_df.convert_dtypes()
columns = ['C_METADATAXML', 'C_COMMENT', 'VALUETYPE_CD', 'M_EXCLUSION_CD', 'C_PATH', 'C_SYMBOL']
gpc_concept_df[columns] = gpc_concept_df[columns].astype('string')
gpc_concept_df.rename(columns=lambda x: x.upper(), inplace=True)
gpc_concept_df['UPDATE_DATE'] = ct
gpc_concept_df['DOWNLOAD_DATE'] = ct
gpc_concept_df['IMPORT_DATE'] = ct
columns = ['UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE']

# df[columns] = df[columns].astype('datetime64[ns]')
gpc_concept_df[columns] = gpc_concept_df[columns].astype('object')

"""
Age ontology update for ACT-SNOWFLAKE
"""

def parse(segment, date_adjust):
     # left part
    new_dim_code = ''
    segments = segment.split('-')
    left_sub_segment = segments[0]
    # left_sub_segment
    if 'GETDATE()' in left_sub_segment:
        new_dim_code += 'CURRENT_DATE'
        if date_adjust == True:
            new_dim_code += ' + (INTERVAL \'1 day\')'
    else:
        return ## error
    # right_sub_segment
    if len(segments) == 2:
        right_sub_segment = segments[1]
        new_split = right_sub_segment.split('*')
        if len(new_split) == 2:
            new_sub_split = new_split[1].split('/')
            if len(new_sub_split) == 2:
                temp = new_sub_split[0].replace('(','').replace(')','')
                new_dim_code += f' - (INTERVAL \'{int(temp) + 1} month\')'
            else:
                temp = new_sub_split[0].replace('(','').replace(')','')
                new_dim_code += f' - (INTERVAL \'{int(temp)} year\')'
        else:
            new_dim_code += ' - (INTERVAL \'1 year\')'

    return new_dim_code
   
age_df = pd.read_csv('demographics/age.tsv',sep='\t')
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
age_df['C_FACTTABLECOLUMN'].loc[(age_df['C_FACTTABLECOLUMN'] == 'concept_cd')] = 'demographic_fact.concept_cd'

for i, row in age_df.iterrows():
    if row['C_OPERATOR'] == 'LIKE':
        continue
    dim_code = row['C_DIMCODE']
    converted_dim_code = ''
    segments = dim_code.split('AND')
    left_part = segments[0]
    if len(segments) == 2:
        right_part = segments[1]    
        converted_dim_code += parse(left_part, True) + ' AND ' + parse(right_part, False)
    else:
        converted_dim_code += parse(left_part, False)
    
    age_df.at[i,'C_DIMCODE'] = converted_dim_code

"""
merge columns
"""

frames = [gpc_concept_df, concept_df, age_df]
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

metadata = 'ACT_DEM_V4'

cur = connection.cursor()
try:
    sql = "delete from " + metadata + '\n' \
        "where C_FULLNAME like '%Demographics%';"
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
        "SET c_facttablecolumn= '" + 'demographic_fact' + ".concept_cd'" + "\n" \
        "WHERE C_TABLE_NAME = '" + 'ACT_DEM_V4' + "';"
    cur.execute(sql)        
finally:
    cur.close()
    
    
    
    