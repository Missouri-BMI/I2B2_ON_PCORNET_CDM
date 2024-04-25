set target_db = '#target_db';
use database identifier($target_db);
CREATE OR REPLACE PROCEDURE delete_all_facts()
RETURNS INTEGER NULL
LANGUAGE SQL
AS
DECLARE
    fact_tables RESULTSET DEFAULT (select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from information_schema.tables where TABLE_SCHEMA = 'I2B2DATA' AND ( LOWER(TABLE_NAME) LIKE '%_fact%' OR LOWER(TABLE_NAME) IN ('patient_dimension', 'visit_dimension', 'provider_dimension')));
    v_sqlStr TEXT DEFAULT '';  
    fact_cur CURSOR FOR fact_tables; 
BEGIN
    for r in fact_cur DO
        if (r.table_type != 'VIEW') then
            execute immediate ('drop table ' || r.table_schema || '.' || r.table_name);
        end if;
    END FOR;
END;

call delete_all_facts();