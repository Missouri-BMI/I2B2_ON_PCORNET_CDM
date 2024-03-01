CREATE OR REPLACE PROCEDURE clear_ont_tables(

)
RETURNS INTEGER NULL
LANGUAGE SQL
AS
$$
DECLARE
    results RESULTSET;
    query VARCHAR DEFAULT 'select c_table_name from table_access';
    tableName VARCHAR;
    ont_table VARCHAR;
BEGIN
    
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur do
        ont_table := record.c_table_name;
        EXECUTE IMMEDIATE ('drop table if exists ' || ont_table);
        
    end for;

    truncate totalnum;
    truncate totalnum_report;
    truncate schemes;
    truncate table_access;
END;
$$
;
