CREATE OR REPLACE PROCEDURE import_ont_tables(
    target_db VARCHAR, 
    target_schema VARCHAR
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
    tabcolumns VARCHAR DEFAULT 'C_TABLE_CD,C_TABLE_NAME,C_PROTECTED_ACCESS,C_ONTOLOGY_PROTECTION,C_HLEVEL,C_FULLNAME,C_NAME,C_SYNONYM_CD,C_VISUALATTRIBUTES,C_TOTALNUM,C_BASECODE,C_METADATAXML,C_FACTTABLECOLUMN,C_DIMTABLENAME,C_COLUMNNAME,C_COLUMNDATATYPE,C_OPERATOR,C_DIMCODE,C_COMMENT,C_TOOLTIP,C_ENTRY_DATE,C_CHANGE_DATE,C_STATUS_CD,VALUETYPE_CD';
BEGIN
    BEGIN
        EXECUTE IMMEDIATE ('INSERT INTO ' || concat(target_db,'.',target_schema,'.','schemes') ||' select * from schemes');
        EXECUTE IMMEDIATE ('INSERT INTO ' || concat(target_db,'.',target_schema,'.','table_access') ||' select ' || tabcolumns || ' from table_access');
    END;
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur do
        ont_table := record.c_table_name;
        EXECUTE IMMEDIATE ('CREATE TABLE ' || concat(target_db,'.',target_schema,'.',ont_table) ||' CLONE '|| ont_table);
        
    end for;

    
END;
$$
;

