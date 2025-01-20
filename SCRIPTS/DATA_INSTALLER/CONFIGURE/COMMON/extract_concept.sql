CREATE OR REPLACE FILE FORMAT {stage_schema}.CSV_FORMAT
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            ESCAPE=NONE
            NULL_IF = ('NULL')
            COMPRESSION=AUTO
            ESCAPE_UNENCLOSED_FIELD=NONE
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            SKIP_HEADER=0;

CREATE OR REPLACE STAGE {stage_schema}.CSV_STAGE FILE_FORMAT = {stage_schema}.CSV_FORMAT;

CREATE OR REPLACE PROCEDURE {stage_schema}.generate_copy_statements()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    table_name STRING;
    results RESULTSET;
    query TEXT; 
BEGIN
    let table_schema := split_part('{stage_schema}', '.' , 2);
    query := 'SELECT table_name FROM information_schema.tables WHERE lower(table_schema) = \'' || table_schema || '\'';
    results := (EXECUTE IMMEDIATE :query);
    let cur CURSOR FOR results;
    for record in cur do
        table_name := record.table_name;
        query := 'COPY INTO @{stage_schema}.CSV_STAGE/' || table_name || '.csv ' ||
                 'FROM {stage_schema}.' || table_name || ' ' ||
                 'OVERWRITE = TRUE ' ||
                 'MAX_FILE_SIZE = 20971520';

        EXECUTE IMMEDIATE :query;
    END FOR;
END;
$$;

-- Generate COPY INTO statements for all tables in the schema
CALL {stage_schema}.generate_copy_statements();