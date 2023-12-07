CREATE OR REPLACE PROCEDURE RUN_FACT_SUMMARAY(

)
RETURNS TABLE()
LANGUAGE SQL
AS
DECLARE
    results RESULTSET;
    query VARCHAR DEFAULT 'select TABLE_NAME, ROW_COUNT From information_schema.tables where lower(table_name) like \'%_fact\' and lower(table_schema) = \'i2b2data\' order by ROW_COUNT DESC';
    tableName VARCHAR;
  
    query_str VARCHAR DEFAULT '';
BEGIN
    
    results := (EXECUTE IMMEDIATE :query);
    return TABLE(results);
END;

CALL RUN_FACT_SUMMARAY();