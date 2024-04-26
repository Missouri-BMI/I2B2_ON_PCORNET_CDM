#!/bin/bash
set -e
SOURCE_DB=I2B2_ETL_TEST
SOURCE_SCHEMA=I2B2_ONT_MOD

TARGET_DB=I2B2_PCORNET_PROD
TARGET_METADATA=i2b2metadata
TARGET_DATA=i2b2data


snowsql -c etl_user -d $SOURCE_DB -s $SOURCE_SCHEMA  <<- EOSQL
    
    TRUNCATE $TARGET_DB.$TARGET_DATA.CONCEPT_DIMENSION;
    
    INSERT INTO $TARGET_DB.$TARGET_DATA.CONCEPT_DIMENSION
    SELECT * FROM CONCEPT_DIMENSION;
    
EOSQL

snowsql -c etl_user -d $TARGET_DB -s $TARGET_METADATA -f /home/scripts/clean-proc.sql
snowsql -c etl_user -d $TARGET_DB -s $TARGET_METADATA -q "call clear_ont_tables();"


snowsql -c etl_user -d $SOURCE_DB -s $SOURCE_SCHEMA -f /home/scripts/import-proc.sql
snowsql -c etl_user -d $SOURCE_DB -s $SOURCE_SCHEMA -q "call import_ont_tables('$TARGET_DB','$TARGET_METADATA');"