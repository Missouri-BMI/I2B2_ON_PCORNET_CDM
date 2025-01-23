-- Follow procedure works fine in any other sql client but not in apache-ant sql.
-- executing in snowflake web console works fine.

CREATE OR REPLACE PROCEDURE load_act_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    table_name STRING;
    results RESULTSET;
    query TEXT; 
BEGIN
    query := 'SELECT C_TABLE_NAME FROM TABLE_ACCESS';
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur
    DO
        table_name := record.C_TABLE_NAME;
        query := 'COPY INTO ' || table_name || ' FROM @CSV_STAGE/' || table_name || '.csv FILE_FORMAT=CSV_FORMAT';
        EXECUTE IMMEDIATE :query;
    END FOR;
END;
$$
;
call load_act_tables();