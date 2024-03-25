snowsql -c etl_user -d I2B2_ETL_TEST <<- EOSQL
    
    CREATE OR REPLACE SCHEMA I2B2_ONT_MOD CLONE I2B2_ONT_RAW;
    
EOSQL

snowsql -c etl_user -d I2B2_ETL_TEST -s I2B2_ONT_MOD -f /home/scripts/harmonize-proc.sql

snowsql -c etl_user -d I2B2_ETL_TEST -s I2B2_ONT_MOD -q "call harmonize_proc();"
