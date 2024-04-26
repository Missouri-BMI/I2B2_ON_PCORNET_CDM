
from kzr_snowflake import kzr_snowflake as ks

# create a json with user, password, and account and provide the path
path = '../environment/cred.json'
role = 'I2B2'
warehouse = 'I2B2_ETL_WH'
database = 'I2B2_PROD'
schema = 'I2B2METADATA'
# USER INPUT END #
# -------------- #

##
# Connecting to snowflake
ks.path = path
ks.role = role
ks.warehouse = warehouse
ks.database = database
ks.schema = schema
ks.connect()

##
sql = 'select distinct FACILITYID from DEIDENTIFIED_PCORNET_CDM.CDM_2023_JULY.DEID_ENCOUNTER'
df = ks.select_into_df(sql)
##
base = r'Visit Details\\Facility'
cfl = ''

for index, row in df.iterrows():
    fl = row['FACILITYID']
    sql = rf"""
        insert into I2B2METADATA.ACT_VISIT_DETAILS_V4
        values (3,
                '\\ACT\\{base}\\{fl}\\',
                '{fl}',
                'N',
                'LA',
                null,
                '{fl}',
                null,
                'encounter_num',
                'visit_dimension',
                'facilityid',
                'T',
                '=',
                '{fl}',
                null,
                '{base}\\{fl}\\',
                '@',
                current_date,
                current_date,
                current_date,
                null,
                null,
                null,
                null,
                null);
        """
    ks.execute(sql)
    cfl = f"{cfl},'{fl}'"

##
cfl = f'({cfl[1:]})'
sql = rf'''
insert into I2B2METADATA.ACT_VISIT_DETAILS_V4
values (2,
        '\\ACT\\{base}\\',
        'Facility ID',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '{cfl}',
        null,
        '{base}',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null);
'''
ks.execute(sql)